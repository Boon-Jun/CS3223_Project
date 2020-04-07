package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

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

        ArrayList<Attribute> attributes = base.getSchema().getAttList();
        compareIndex = new ArrayList<>();

        for (int i = 0; i < attributes.size(); i++) {
            Attribute attribute = attributes.get(i);
            compareIndex.add(base.getSchema().indexOf(attribute));
        }
        sorted = new ExternalSort("distinct", base, compareIndex, false, this.numBuff);
        if (!sorted.open()) {
            System.out.printf("Unable to open sorted");
            return false;
        }
        curs = 0;
        inbatch = sorted.next();
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
                if (previousTuple == null || Tuple.compareTuples(previousTuple, inbatch.get(curs), compareIndex) != 0) {
                    outbatch.add(inbatch.get(curs));
                    previousTuple = inbatch.get(curs);
                }
                curs++;
            } else {
                inbatch = sorted.next();
                curs = 0;
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
        sorted.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Distinct newdistinct = new Distinct(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newdistinct.setSchema(newSchema);
        return newdistinct;
    }
}
