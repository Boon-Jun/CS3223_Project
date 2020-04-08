package qp.operators;

import qp.utils.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class SortMergeJoin extends Join{
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    ExternalSort sortedLeft; //External Sort Operator
    TupleReader leftReader; //Reader for materialized left hand side
    TupleReader rightReader; //Reader for materialized right hand side

    Batch outbatch;                 // Buffer page for output

    Batch leftBatch;                // Buffer page for left output stream;
    int lcurs;                      // Cursor for left side buffer

    ArrayList<Tuple> rightPartition;//partition of equivalent values
    int rpcurs;                     // Cursor for right partition;

    Tuple currLeft;//pointer to sorted left operator
    Tuple currRight;//pointer to tuple to sorted right operator

    String rfname;
    String lfname;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
    }

    private Tuple advanceLeft() {
        if (leftReader != null) {
            //Left hand side is materialized and can be read with leftReader
            return leftReader.next();
        }

        if (lcurs == 0) {
            leftBatch = sortedLeft.next();
        }

        if (leftBatch == null) {
            return null;
        }
        Tuple tuple = leftBatch.get(lcurs);
        lcurs++;

        if (lcurs >= leftBatch.size()) {
            lcurs = 0;
        }
        return tuple;
    }

    private Tuple advanceRight() {
        return rightReader.next();
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
        Batch page;
        ExternalSort sortedRight = new ExternalSort("right", right, rightindex, false, numBuff);
        rfname = "SMJtemp_right";
        if (!sortedRight.open()) {
            System.out.printf("Unable to open sorted right");
            return false;
        }

        //Materializing the right hand side
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
            while ((page = sortedRight.next()) != null) {
                out.writeObject(page);
            }
            out.close();
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error writing to temporary file");
            return false;
        }
        if (!sortedRight.close())
            return false;

        sortedLeft = new ExternalSort("left", left, leftindex, false, numBuff);
        if (!sortedLeft.open()) {
            System.out.printf("Unable to open sorted left");
            return false;
        }

        rightReader = new TupleReader(rfname, this.batchsize);
        if (!rightReader.open()) {
            System.out.println("SortMergeJoin: Unable to open right reader");
            this.close();
            return false;
        }
        if (numBuff == 3) {
            //If there are only 3 buffers available, materialize the left hand side
            lfname = "SMJtemp_left";
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
                while ((page = sortedLeft.next()) != null) {
                    out.writeObject(page);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error writing to temporary file");
                return false;
            }
            if (!sortedLeft.close())
                return false;
            leftReader = new TupleReader(rfname, this.batchsize);
            if (!leftReader.open()) {
                System.out.println("SortMergeJoin: Unable to open left reader");
                this.close();
                return false;
            }
        }
        lcurs = 0;

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
        File f = new File(rfname);
        f.delete();
        f = new File(lfname);
        f.delete();
        if (leftReader != null) {
            leftReader.close();
        } else {
            sortedLeft.close();
        }
        rightReader.close();
        return true;
    }

}
