package qp.operators;

import qp.utils.*;

public class OrderBy extends Operator{

    private enum OrderType{
        ASC,DESC
    }
    private Attribute attribute;
    private OrderType orderType;

    public OrderBy(Attribute attribute, OrderType orderType) {
        super(OpType.SORT);
        this.attribute = attribute;
        this.orderType = orderType;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public OrderType getOrderType() {
        return orderType;
    }
}
