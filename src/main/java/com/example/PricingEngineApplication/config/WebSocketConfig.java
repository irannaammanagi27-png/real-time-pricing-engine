package com.example.PricingEngineApplication.config;

import com.example.PricingEngineApplication.controller.PricingWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class WebSocketConfig {

    @Bean
    public WebSocketClient webSocketClient() {
        return new ReactorNettyWebSocketClient();
    }

    @Bean
    public HandlerMapping webSocketMapping(
            PricingWebSocketHandler handler) {

        Map<String, WebSocketHandler> urlMap =
                new HashMap<>();

        urlMap.put(
                "/ws/prices",
                handler
        );

        SimpleUrlHandlerMapping mapping =
                new SimpleUrlHandlerMapping();

        mapping.setUrlMap(urlMap);
        mapping.setOrder(-1);

        return mapping;
    }

    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}