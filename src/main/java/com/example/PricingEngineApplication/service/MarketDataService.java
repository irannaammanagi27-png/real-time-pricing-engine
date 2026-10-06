package com.example.PricingEngineApplication.service;

import com.example.PricingEngineApplication.client.MarketDataWebSocketClient;
import com.example.PricingEngineApplication.client.PricingBroadcaster;
import com.example.PricingEngineApplication.model.MarketTick;
import com.example.PricingEngineApplication.model.PriceSnapshot;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import jakarta.annotation.PostConstruct;

import java.util.List;

@Service
public class MarketDataService {

    private final PriceCalculationService calculationService;
    private final MarketDataWebSocketClient webSocketClient;
    private final PricingBroadcaster pricingBroadcaster;

    public MarketDataService(
            PriceCalculationService calculationService,
            MarketDataWebSocketClient webSocketClient,
            PricingBroadcaster pricingBroadcaster) {

        this.calculationService = calculationService;
        this.webSocketClient = webSocketClient;
        this.pricingBroadcaster = pricingBroadcaster;
    }

    @PostConstruct
    public void start() {

        System.out.println(
                "Starting Market Data Service..."
        );

        List<String> symbols =
                webSocketClient.getSymbols();

        /*
         * Step 1:
         * Restore all previous state from Redis.
         */
        System.out.println(
                "Restoring price history from Redis..."
        );

        Flux.fromIterable(symbols)
                .flatMap(
                        calculationService::restoreSymbol
                )
                .then()
                .doOnSuccess(ignored -> {

                    System.out.println(
                            "Redis restoration completed."
                    );

                    /*
                     * Step 2:
                     * Only after restoration is complete,
                     * connect to upstream market WebSocket.
                     */
                    System.out.println(
                            "Connecting to upstream market WebSocket..."
                    );

                    webSocketClient.connect(
                            this::processTick
                    );
                })
                .doOnError(error ->
                        System.err.println(
                                "Startup restoration failed: "
                                        + error.getMessage()
                        )
                )
                .subscribe();
    }

    public void processTick(
            MarketTick tick) {

        PriceSnapshot snapshot =
                calculationService.process(tick);

        if (snapshot == null) {
            return;
        }

        /*
         * Send the calculated price to all
         * subscribed downstream clients.
         */
        pricingBroadcaster.broadcast(
                snapshot
        );
    }
}