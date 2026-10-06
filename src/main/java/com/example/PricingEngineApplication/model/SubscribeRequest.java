package com.example.PricingEngineApplication.model;

import java.util.List;

public class SubscribeRequest {

    private String action;
    private List<String> symbols;

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public List<String> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<String> symbols) {
        this.symbols = symbols;
    }
}