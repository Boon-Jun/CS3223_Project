package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class OrderBy extends Operator {

    private Operator base;                 // Base table to project
    private ArrayList<Attribute> attrset;  // Set of attributes to project
    private ArrayList<Attribute> compareAttri;  // Set of attributes to orderBy
    private int batchsize;                 // Number of tuples per outbatch
    private int numBuff;


    private boolean isDesc;           // Descending or ascending

    /**
     * The following fields are required during execution
     * * of the Project Operator
     **/
    private Batch inbatch;
    private Batch outbatch;
    private int curs;
    private Tuple previousTuple;
    private ArrayList<Integer> compareIndex;  //Index of attributes to be sorted by

    ExternalSort sorted;


    public OrderBy(Operator base, ArrayList<Attribute> attrset,  ArrayList<Attribute> compareAttri, boolean isDesc) {
        super(OpType.SORT);
        this.attrset = attrset;
        this.isDesc = isDesc;
        this.base = base;
        this.compareAttri = compareAttri;
    }

    public boolean getOrderType() {
        return isDesc;
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


        compareIndex = new ArrayList<>();

        for (int i = 0; i < compareAttri.size(); i++) {
            Attribute attribute = compareAttri.get(i);
            compareIndex.add(base.getSchema().indexOf(attribute));
        }
        sorted = new ExternalSort("order", base, compareIndex, this.isDesc, this.numBuff);
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
                outbatch.add(inbatch.get(curs));
                previousTuple = inbatch.get(curs);
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

        boolean order = this.isDesc;


        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        OrderBy newOrderBy = new OrderBy(newbase, newattr, this.compareAttri, order);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newOrderBy.setSchema(newSchema);
        return newOrderBy;
    }
}
