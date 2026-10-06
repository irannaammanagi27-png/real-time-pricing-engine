package com.example.PricingEngineApplication.controller;

import com.example.PricingEngineApplication.client.PricingBroadcaster;
import com.example.PricingEngineApplication.model.PriceSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;

import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PricingWebSocketHandler implements WebSocketHandler {

    private final PricingBroadcaster broadcaster;
    private final ObjectMapper objectMapper;

    private final Map<WebSocketSession,Set<String>> clients = new ConcurrentHashMap<>();

    public PricingWebSocketHandler(PricingBroadcaster broadcaster,ObjectMapper objectMapper) {
        this.broadcaster = broadcaster;
        this.objectMapper = objectMapper;
        broadcaster.setHandler(this);
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {

        System.out.println("Client connected: " + session.getId()
        );
        clients.put(session,ConcurrentHashMap.newKeySet()
        );

        return session.receive().map(message ->message.getPayloadAsText())
                .doOnNext(message -> {
                    handleClientMessage(session,message);
                })
                .doFinally(signal -> {
                    clients.remove(session);
                    System.out.println(
                            "Client disconnected: "
                                    + session.getId()
                    );
                })
                .then();
    }
    private void handleClientMessage(
            WebSocketSession session,
            String message) {

        try {
            ClientSubscription subscription =
                    objectMapper.readValue(
                            message,
                            ClientSubscription.class
                    );

            if (subscription.symbols() != null) {
                clients
                        .get(session)
                        .clear();

                clients
                        .get(session)
                        .addAll(
                                subscription.symbols()
                        );
            }

        } catch (Exception e) {
            System.err.println(
                    "Invalid client subscription: "
                            + e.getMessage()
            );
        }
    }

    public void broadcast(
            PriceSnapshot snapshot) {
        clients.forEach(
                (session, subscriptions) -> {
                    if (!session.isOpen()) {
                        return;
                    }
                    if (!subscriptions.contains(
                            snapshot.getSymbol())) {
                        return;
                    }
                    try {

                        String json =
                                objectMapper.writeValueAsString(
                                        snapshot
                                );
                        session
                                .send(
                                        Mono.just(
                                                session.textMessage(
                                                        json
                                                )
                                        )
                                )
                                .subscribe();

                    } catch (Exception e) {

                        System.err.println(
                                "Broadcast error: "
                                        + e.getMessage()
                        );
                    }
                }
        );
    }
    public record ClientSubscription(
            String action,
            Set<String> symbols
    ) {
    }
}