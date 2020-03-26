package qp.operators;

import qp.utils.*;

import java.util.ArrayList;
import java.util.Collections;

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
}
