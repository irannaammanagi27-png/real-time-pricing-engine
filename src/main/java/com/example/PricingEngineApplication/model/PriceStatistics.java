package com.example.PricingEngineApplication.model;

public class PriceStatistics {

    private double high24h;
    private double low24h;
    private double change24h;

    public double getHigh24h() {
        return high24h;
    }

    public void setHigh24h(double high24h) {
        this.high24h = high24h;
    }

    public double getLow24h() {
        return low24h;
    }

    public void setLow24h(double low24h) {
        this.low24h = low24h;
    }

    public double getChange24h() {
        return change24h;
    }

    public void setChange24h(double change24h) {
        this.change24h = change24h;
    }
}