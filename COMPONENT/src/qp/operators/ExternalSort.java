package qp.operators;

import qp.utils.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

public class ExternalSort extends Operator{
    private boolean reverse;
    private int numBuffer; //number of buffers available
    private int batchSize; //number of tuples that can be stored in the batch
    private int pass = 0;

    private String id;
    private Operator base;
    private final ArrayList<Integer> compareIndex; //Index of columns to sort by
    private ArrayList<String> tempFiles; //Contains pages that are stored on the disk
    ArrayList<TupleReader> curr_readers; //Input tuple readers(Each tuple reader is allocated 1 input buffer during merging)
    PriorityQueue<TupleIndexPair> pq; //PQ is used to select tuples from the respective input buffers during merging phase

    public ExternalSort(String id, Operator base, ArrayList<Integer> compareIndex, boolean reverse, int numBuffer) {
        super(OpType.SORT);
        this.base = base;
        this.numBuffer = numBuffer;
        this.compareIndex = compareIndex;
        this.reverse = reverse;
        int tupleSize = base.getSchema().getTupleSize();
        this.batchSize = Batch.getPageSize()/tupleSize;
        this.tempFiles = new ArrayList<>();
        this.id = id;
        curr_readers = new ArrayList<>();
    }

    private void deleteTempFiles() {
        for (int x = 0; x < this.tempFiles.size(); x++) {
            File f = new File(this.tempFiles.get(x));
            f.delete();
        }
    }

    private void writeTuplesToOutput(ArrayList<Tuple> tuples, String fileName) {
        TupleWriter writer = new TupleWriter(fileName, this.batchSize);
        if (!writer.open()) {
            System.out.printf("%s:writing SM file error\n", fileName);
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
            System.out.printf("Unable to open operator to generate Sorted Runs\n");
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
                writeTuplesToOutput(inMemoryTuples, output_file);
                tempFiles.add(output_file);
                runCount++;
            }
        } while (!lastBatch);

        this.pass++;
        this.base.close();
    }

    private void merge_pq_setup() {
        Comparator<TupleIndexPair> custom_comparator = new Comparator<TupleIndexPair>() {
            @Override
            public int compare(TupleIndexPair o1, TupleIndexPair o2) {
                int result = Tuple.compareTuples(o1.tuple, o2.tuple, compareIndex, compareIndex);
                return reverse ? -result: result;
            }
        };
        pq = new PriorityQueue<>(numBuffer - 1, custom_comparator);
        //Populate PQ with first tuple from each buffer
        for (int x = 0; x < curr_readers.size(); x++) {
            Tuple tuple = curr_readers.get(x).next();
            assert(tuple != null);
            pq.add(new TupleIndexPair(tuple, x));
        }
    }

    private void merge(String output_file) {
        TupleWriter writer = new TupleWriter(output_file, this.batchSize);

        if (!writer.open()) {
            System.out.printf("%s:writing merged file error\n", output_file);
            System.exit(1);
        }

        merge_pq_setup();
        //Merge and place each element into the output buffer
        while (!pq.isEmpty()) {
            TupleIndexPair tupleIndexPair = pq.poll();
            writer.next(tupleIndexPair.tuple);
            Tuple nextTuple = curr_readers.get(tupleIndexPair.index).next();
            if (nextTuple != null) {
                pq.add(new TupleIndexPair(nextTuple, tupleIndexPair.index));
            }
        }
        writer.close();
    }

    //single pass over all pages
    private void mergePass() {
        ArrayList<String> next_sorted_temp_runs = new ArrayList<>();
        int run_count = 0;
        int x = 0;

        while (x < this.tempFiles.size()) {
            do {
                //Add pages into buffer until input buffers are full
                String tempFileName = this.tempFiles.get(x);
                TupleReader reader = new TupleReader(tempFileName, this.batchSize);
                if (!reader.open()) {
                    System.out.printf("%s: Unable to open file to during merging\n", tempFileName);
                    System.exit(1);
                }
                curr_readers.add(reader);
                x++;
            } while (x < this.tempFiles.size() && x % (this.batchSize - 1) != 0);

            String output_file = "SMtempRun-" + this.id + "-" + this.pass + "-" + run_count;
            merge(output_file);
            next_sorted_temp_runs.add(output_file);
            run_count++;
            curr_readers.clear();
        }
        this.pass++;
        deleteTempFiles();
        this.tempFiles = next_sorted_temp_runs;
    }

    public boolean open() {
        generateSortedRuns();
        while (tempFiles.size() > numBuffer - 1) {
            mergePass();
        }
        //Load pages in disk into buffer
        for (int x = 0; x < tempFiles.size(); x++) {
            String tempFileName = this.tempFiles.get(x);
            TupleReader reader = new TupleReader(tempFileName, this.batchSize);
            if (!reader.open()) {
                System.out.printf("%s: Unable to open file for final merge\n", tempFileName);
                System.exit(1);
            }
            curr_readers.add(reader);
            x++;
        }
        merge_pq_setup();
        return true;
    }

    public Batch next() {
        Batch outBatch = new Batch(this.batchSize);
        while (!pq.isEmpty()) {
            TupleIndexPair tupleIndexPair = pq.poll();
            outBatch.add(tupleIndexPair.tuple);
            Tuple nextTuple = curr_readers.get(tupleIndexPair.index).next();
            if (nextTuple != null) {
                pq.add(new TupleIndexPair(nextTuple, tupleIndexPair.index));
            }
            if (outBatch.isFull()) {
                return outBatch;
            }
        }
        if (outBatch.isEmpty()) {
            outBatch = null;
        }
        return outBatch;
    }

    public boolean close() {
        for (int x = 0; x < curr_readers.size(); x++) {
            curr_readers.get(x).close();
        }
        deleteTempFiles();
        return true;
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
