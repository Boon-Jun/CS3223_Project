package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.util.ArrayList;

public class SortMergeJoin extends Join{
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    ExternalSort sortedLeft, sortedRight;

    Batch outbatch;                 // Buffer page for output

    Batch leftBatch;
    Batch rightBatch;               // Buffer page for right input stream
    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer

    ArrayList<Tuple> rightPartition;//partition of equivalent values
    int rpcurs;                     // Cursor for right partition;

    Tuple currLeft;
    Tuple currRight;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
    }

    private Tuple advanceLeft() {
        if (lcurs == 0) {
            leftBatch = sortedLeft.next();
        }

        if (leftBatch == null) {
            return null;
        }
        Tuple tuple = leftBatch.get(lcurs);
        lcurs++;

        if (lcurs > leftBatch.size()) {
            lcurs = 0;
        }
        return tuple;
    }

    private Tuple advanceRight() {
        if (rcurs == 0) {
            rightBatch = sortedRight.next();
        }
        if (rightBatch == null) {
            return null;
        }
        Tuple tuple = rightBatch.get(rcurs);
        rcurs++;

        if (rcurs > rightBatch.size()) {
            rcurs = 0;
        }
        return tuple;
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch (Note: for output) **/
        int tuplesize = schema.getTupleSize();
        numBuff = this.getNumBuff();

        batchsize = Batch.getPageSize() / tuplesize;
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        sortedLeft = new ExternalSort("left", left, leftindex, false, numBuff);
        sortedRight = new ExternalSort("right", right, rightindex, false, numBuff);
        if (!sortedLeft.open()) {
            System.out.printf("Unable to open sorted left");
            return false;
        }
        if (!sortedRight.open()) {
            System.out.printf("Unable to open right sorted right");
            sortedLeft.close();
            return false;
        }

        lcurs = 0;
        rcurs = 0;

        currLeft = advanceLeft();
        currRight = advanceRight();

        rightPartition = new ArrayList<>();
        return true;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        outbatch = new Batch(batchsize);
        while (currLeft != null && (currRight != null || !rightPartition.isEmpty())) {
            if (!rightPartition.isEmpty()) {
                while (Tuple.compareTuples(currLeft, rightPartition.get(0), leftindex, rightindex) == 0) {
                    while (rpcurs < rightPartition.size()) {
                        outbatch.add(currLeft.joinWith(rightPartition.get(rpcurs)));
                        rpcurs++;
                        if (outbatch.isFull())
                            return outbatch;
                    }
                    rpcurs = 0;
                    currLeft = advanceLeft();
                    if (currLeft == null) {
                        return outbatch;
                    }
                }
                rightPartition.clear();
                if (currRight == null) {
                    return outbatch;
                }
            }

            while (Tuple.compareTuples(currLeft, currRight, leftindex, rightindex) < 0) {
                currLeft = advanceLeft();
                if (currLeft == null) {
                    if (!outbatch.isEmpty()) {
                        return outbatch;
                    }
                    return null;
                }
            }

            while (Tuple.compareTuples(currLeft, currRight, leftindex, rightindex) > 0) {
                currRight = advanceRight();
                if (currRight == null) {
                    if (!outbatch.isEmpty()) {
                        return outbatch;
                    }
                    return null;
                }
            }

            while (Tuple.compareTuples(currLeft, currRight, leftindex, rightindex) == 0) {
                rightPartition.add(currRight);
                currRight = advanceRight();
                if (currRight == null) {
                    break;
                }
            }
        }

        return null;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        sortedLeft.close();
        sortedRight.close();
        return true;
    }

}
