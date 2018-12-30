package com.hyman.rabbitmqdemo.rabbitmqSpring.messageConverter;

import java.math.BigDecimal;

public class Order {
    /**
     * 订单编号
     **/
    private String orderId;

    /**
     * 订单金额
     **/
    private BigDecimal orderAmount;

    public Order() {

    }

    public Order(String orderId, BigDecimal orderAmount) {
        this.orderId = orderId;
        this.orderAmount = orderAmount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public BigDecimal getOrderAmount() {
        return orderAmount;
    }

    public void setOrderAmount(BigDecimal orderAmount) {
        this.orderAmount = orderAmount;
    }
}
