package com.example.PricingEngineApplication.client;


import org.springframework.stereotype.Component;

import com.example.PricingEngineApplication.controller.PricingWebSocketHandler;
import com.example.PricingEngineApplication.model.PriceSnapshot;

@Component
public class PricingBroadcaster {

    private PricingWebSocketHandler handler;

    public void setHandler(
            PricingWebSocketHandler handler) {

        this.handler = handler;
    }

    public void broadcast(
            PriceSnapshot snapshot) {

        if (handler != null) {

            handler.broadcast(snapshot);
        }
    }
}