package qp.utils;

import java.util.ArrayList;
import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple> {

    private ArrayList<Integer> leftIndex, rightIndex;
    private boolean reverse = false;

    public TupleComparator(ArrayList<Integer> leftIndex, ArrayList<Integer> rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    public TupleComparator(ArrayList<Integer> leftIndex, ArrayList<Integer> rightIndex, boolean reverse) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
        this.reverse = reverse;
    }

    @Override
    public int compare(Tuple o1, Tuple o2) {
        int result = Tuple.compareTuples(o1, o2, leftIndex, rightIndex);
        return reverse ? -result: result;
    }
}
