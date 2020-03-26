package qp.operators;

import qp.utils.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

public class SortMerge {
    private boolean reverse;
    private int numBuffer; //number of buffers available
    private int batchSize; //number of tuples that can be stored in the batch
    private int pass = 0;

    private String id;
    private Operator base;
    private final ArrayList<Integer> compareIndex; //Index of columns to sort by
    private ArrayList<String> tempFiles;

    public SortMerge(String id, Operator base, ArrayList<Integer> compareIndex, boolean reverse, int numBuffer) {
        this.base = base;
        this.numBuffer = numBuffer;
        this.compareIndex = compareIndex;
        this.reverse = reverse;
        int tupleSize = base.getSchema().getTupleSize();
        this.batchSize = Batch.getPageSize()/tupleSize;
        this.tempFiles = new ArrayList<>();
        this.id = id;
    }

    private void deleteTempFiles() {
        for (int x = 0; x < this.tempFiles.size(); x++) {
            File f = new File(this.tempFiles.get(x));
            f.delete();
        }
    }

    private void writeTuplesArrayOutput(ArrayList<Tuple> tuples, String fileName) {
        TupleWriter writer = new TupleWriter(fileName, this.batchSize);
        if (!writer.open()) {
            System.out.printf("%s:writing SM file error", fileName);
            System.exit(1);
        }
        for (int x = 0; x < tuples.size(); x++) {
            writer.next(tuples.get(x));
        }
        writer.close();
    }

    private void generateSortedRuns() {
        Batch nextBatch;
        int runCount = 0;

        if (!this.base.open()) {
            System.out.printf("Unable to open operator to generate Sorted Runs");
            System.exit(1);
        }

        int batchCount;
        boolean lastBatch = false;
        do {
            batchCount = 0;
            ArrayList<Tuple> inMemoryTuples = new ArrayList<>();
            while (batchCount < this.numBuffer) {
                nextBatch = this.base.next();
                if (nextBatch == null) {
                    lastBatch = true;
                    break;
                }
                for (int x = nextBatch.size() - 1; x >= 0; x--) {
                    inMemoryTuples.add(nextBatch.get(x));
                    nextBatch.remove(x);//Simulate the situation where we are operating with limited buffers
                }
                batchCount++;
            }
            if (!inMemoryTuples.isEmpty()) {
                Collections.sort(inMemoryTuples, new TupleComparator(this.compareIndex, this.compareIndex, this.reverse));
                String output_file = "SMtempRun-" + this.id + "_" + this.pass + "-" + runCount;
                writeTuplesArrayOutput(inMemoryTuples, output_file);
                tempFiles.add(output_file);
                runCount++;
            }
        } while (!lastBatch);

        this.pass++;
        this.base.close();
    }

    private void merge(ArrayList<TupleReader> readers, String output_file) {
        TupleWriter writer = new TupleWriter(output_file, this.batchSize);
        Comparator<TupleIndexPair> custom_comparator = new Comparator<TupleIndexPair>() {
            @Override
            public int compare(TupleIndexPair o1, TupleIndexPair o2) {
                int result = Tuple.compareTuples(o1.tuple, o2.tuple, compareIndex, compareIndex);
                return reverse ? -result: result;
            }
        };

        PriorityQueue<TupleIndexPair> pq = new PriorityQueue<>(numBuffer - 1, custom_comparator);

        if (!writer.open()) {
            System.out.printf("%s:writing merged file error", output_file);
            System.exit(1);
        }

        //Populate PQ with first tuple from each buffer
        for (int x = 0; x < readers.size(); x++) {
            Tuple tuple = readers.get(x).next();
            assert(tuple != null);
            pq.add(new TupleIndexPair(tuple, x));
        }

        //Merge and place each element into the output buffer
        while (!pq.isEmpty()) {
            TupleIndexPair tupleIndexPair = pq.poll();
            writer.next(tupleIndexPair.tuple);
            Tuple nextTuple = readers.get(tupleIndexPair.index).next();
            if (nextTuple != null) {
                pq.add(new TupleIndexPair(nextTuple, tupleIndexPair.index));
            }
        }
        writer.close();
    }

    private void mergeRuns() {
        ArrayList<String> next_sorted_temp_runs = new ArrayList<>();
        ArrayList<TupleReader> readers = new ArrayList<>();
        int run_count = 0;
        int x = 0;

        while (x < this.tempFiles.size()) {
            do {
                String tempFileName = this.tempFiles.get(x);
                TupleReader reader = new TupleReader(tempFileName, this.batchSize);
                if (!reader.open()) {
                    System.out.printf("%s: Unable to open file to during merging", tempFileName);
                    System.exit(1);
                }
                readers.add(reader);
                x++;
            } while (x < this.tempFiles.size() && x % (this.batchSize - 1) != 0);
            String output_file = "SMtempRun-" + this.id + "-" + this.pass + "-" + run_count;
            merge(readers, output_file);
            next_sorted_temp_runs.add(output_file);
            run_count++;
        }
        this.pass++;
        deleteTempFiles();
        this.tempFiles = next_sorted_temp_runs;
    }

    private class TupleIndexPair {
        Tuple tuple;
        int index;

        private TupleIndexPair(Tuple tuple, int index) {
            this.tuple = tuple;
            this.index = index;
        }
    }
}
