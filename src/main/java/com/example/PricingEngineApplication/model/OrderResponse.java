package com.example.PricingEngineApplication.model;

public class OrderResponse {
    private String orderId;
    private String symbol;
    private String side;
    private double quantity;
    private double executionPrice;
    private String status;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }

    public double getQuantity() {
        return quantity;
    }

    public void setQuantity(double quantity) {
        this.quantity = quantity;
    }

    public double getExecutionPrice() {
        return executionPrice;
    }

    public void setExecutionPrice(double executionPrice) {
        this.executionPrice = executionPrice;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}