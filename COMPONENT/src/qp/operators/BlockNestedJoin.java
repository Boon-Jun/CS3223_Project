/**
 * Block Nested Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    ArrayList<Batch> leftbatches;   // Buffer pages for left input stream (since it is Block Nested Join)
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int blocksize;                  // Number of pages in a blocks

    int lcurs;                      // Cursor for left side buffer
    int lbcurs;                      // Cursor for Left side buffer array list
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached


    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch (Note: for output) **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** block size for left batches in **/
        blocksize = numBuff - 2; // Exclude input buffer for right relation and output buffer

        /** ArrayList for blocksize leftnatches **/
        leftbatches = new ArrayList<>();

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        lbcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("NestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        if (eosl && lbcurs == 0 && lcurs == 0) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lbcurs == 0 && lcurs == 0 && eosr == true) {
                // End join if all leftBatches are consumed
                if (eosl) {
                    return outbatch;
                }
                /** new block is to be fetched**/
                leftbatches = new ArrayList<>();
                for (int i = 0; i < blocksize; i++) {
                    Batch tempLeftBatch = left.next();
                    if (tempLeftBatch == null && i == 0) {
                        eosl = true;
                        return outbatch;
                    } else if(tempLeftBatch == null){
                        eosl = true;
                    } else {
                        leftbatches.add(tempLeftBatch);
                    }
                }

                /** Whenever a new left page came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("NestedJoin:error in reading the file");
                    System.exit(1);
                }
            }
            while (eosr == false) {
                try {
                    // Read in new right batch if the left block is done with the current right block
                    if (rcurs == 0 && lbcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    for (int k = lbcurs; k < leftbatches.size(); ++k) {
                        Batch leftbatch = leftbatches.get(k);
                        for (int i = lcurs; i < leftbatch.size(); ++i) {
                            for (int j = rcurs; j < rightbatch.size(); ++j) {
                                Tuple lefttuple = leftbatch.get(i);
                                Tuple righttuple = rightbatch.get(j);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        boolean leftBatchesCursorFinished = (k == (leftbatches.size() - 1));
                                        boolean leftBatchCursorFinished = (i == (leftbatch.size() - 1));
                                        boolean rightBatchCursorFinished = (j == (rightbatch.size() - 1));

                                        if (leftBatchesCursorFinished && leftBatchCursorFinished && rightBatchCursorFinished) {
                                            lbcurs = 0;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else if (leftBatchesCursorFinished && !leftBatchCursorFinished && rightBatchCursorFinished) {
                                            lbcurs = k;
                                            lcurs = i + 1;
                                            rcurs = 0;
                                        } else if (leftBatchesCursorFinished && leftBatchCursorFinished && !rightBatchCursorFinished) {
                                            lbcurs = k;
                                            lcurs = i;
                                            rcurs = j + 1;
                                        } else if (leftBatchesCursorFinished && !leftBatchCursorFinished && !rightBatchCursorFinished) {
                                            lbcurs = k;
                                            lcurs = i;
                                            rcurs = j + 1;
                                        } else if (!leftBatchesCursorFinished && leftBatchCursorFinished && rightBatchCursorFinished) {
                                            lbcurs = k + 1;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else if (!leftBatchesCursorFinished && !leftBatchCursorFinished && rightBatchCursorFinished) {
                                            lbcurs = k;
                                            lcurs = i + 1;
                                            rcurs = 0;
                                        } else if (!leftBatchesCursorFinished && leftBatchCursorFinished && !rightBatchCursorFinished) {
                                            lbcurs = k;
                                            lcurs = i;
                                            rcurs = j + 1;
                                        } else if (!leftBatchesCursorFinished && !leftBatchCursorFinished && !rightBatchCursorFinished) {
                                            lbcurs = k;
                                            lcurs = i;
                                            rcurs = j + 1;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    lbcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("NestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("NestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("NestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }

}