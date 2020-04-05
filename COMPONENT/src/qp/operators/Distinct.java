package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class Distinct extends Operator {
    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    int numBuff;

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;
    int curs;
    Tuple previousTuple;
    ArrayList<Integer> compareIndex;

    ExternalSort sorted;

    public Distinct(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }


    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** Table is required to be  to be materialized
         ** for sort based "distinct" operation to be performed
         **/
        if (!base.open()) {
            return false;
        } else {
            String filename = "DISTINCTtemp";
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
                Batch basePage;

                while ((basePage = base.next()) != null) {
                    out.writeObject(basePage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("Distinct: Error writing to temporary file");
                return false;
            }
            if (!base.close())
                return false;
        }
        ArrayList<Attribute> attributes = base.getSchema().getAttList();
        compareIndex = new ArrayList<>();

        for (int i = 0; i < attributes.size(); i++) {
            Attribute attribute = attributes.get(i);
            compareIndex.add(base.getSchema().indexOf(attribute));
        }
        this.sorted = new ExternalSort("distinct",base, compareIndex, false, numBuff);
        curs = 0;
        inbatch = sorted.next();
        previousTuple = inbatch.get(0);

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        if (inbatch == null) {
            return null;
        }
        while (!outbatch.isFull()) {
            if (curs < inbatch.size()) {
                if (Tuple.compareTuples(previousTuple, inbatch.get(curs), compareIndex) == 0) {
                    outbatch.add(inbatch.get(curs));
                    previousTuple = inbatch.get(curs);
                }
                curs++;
            } else {
                inbatch = sorted.next();
                if (inbatch == null) {
                    break;
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        sorted.close();
        return true;
    }

}
