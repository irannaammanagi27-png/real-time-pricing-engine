package com.example.PricingEngineApplication.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketTick {

    private String symbol;

    @JsonProperty("bid")
    private double buy;

    @JsonProperty("ask")
    private double sell;

    @JsonProperty("high")
    private double high24h;

    @JsonProperty("low")
    private double low24h;

    @JsonProperty("percentage")
    private double change24h;

    private long timestamp;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getBuy() {
        return buy;
    }

    public void setBuy(double buy) {
        this.buy = buy;
    }

    public double getSell() {
        return sell;
    }

    public void setSell(double sell) {
        this.sell = sell;
    }

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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}