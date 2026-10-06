package com.example.PricingEngineApplication.client;

import com.example.PricingEngineApplication.model.MarketTick;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.util.List;
import java.util.function.Consumer;

@Component
public class MarketDataWebSocketClient {

    @Value("${upstream.websocket.url}")
    private String websocketUrl;

    @Value("${upstream.api-key}")
    private String apiKey;

    @Value("${upstream.secret-key}")
    private String secretKey;

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    private final List<String> symbols = List.of(
            "AUDUSD",
            "AUDCAD",
            "AUDCHF",
            "AUDJPY",
            "AUDNZD",
            "AUDSGD",

            "CADCHF",
            "CADJPY",

            "CHFJPY",
            "CHFSGD",

            "EURAUD",
            "EURCAD",
            "EURCHF",
            "EURGBP",
            "EURJPY",
            "EURNZD",
            "EURUSD",

            "GBPAUD",
            "GBPCAD",
            "GBPCHF",
            "GBPJPY",
            "GBPNZD",
            "GBPUSD",

            "NZDCAD",
            "NZDCHF",
            "NZDJPY",
            "NZDUSD",

            "USDCAD",
            "USDCHF",
            "USDCNH",
            "USDJPY",
            "USDMXN",
            "USDNOK",
            "USDPLN",
            "USDSEK",
            "USDSGD",
            "USDTRY",
            "USDZAR",

            "EURTRY",
            "GBPTRY",

            "NOKJPY",
            "SEKJPY",
            "SGDJPY",
            "ZARJPY",

            "XAUUSD",
            "XAGUSD",

            "BTCUSD",
            "ETHUSD",
            "BNBUSD",
            "SOLUSD",
            "XRPUSD",
            "ADAUSD",
            "DOGUSD",
            "DOTUSD",
            "LTCUSD",
            "BCHUSD",
            "XLMUSD",
            "TRXUSD",
            "UNIUSD",
            "FILUSD",
            "AVXUSD"
    );

    /*
     * Returns all symbols that the engine subscribes to.
     */
    public List<String> getSymbols() {
        return symbols;
    }

    /*
     * Connect to upstream market WebSocket.
     */
    public void connect(Consumer<MarketTick> tickConsumer) {
        connectWithRetry(tickConsumer, 0);
    }

    private void connectWithRetry(
            Consumer<MarketTick> tickConsumer,
            int attempt) {

        HttpClient client = HttpClient.create();

        client.websocket()
                .uri(websocketUrl)
                .handle((inbound, outbound) -> {

                    System.out.println(
                            "Connected to market WebSocket"
                    );

                    Mono<Void> authentication =
                            outbound
                                    .sendString(
                                            Mono.just(
                                                    createAuthMessage()
                                            )
                                    )
                                    .then();

                    Mono<Void> receiving =
                            inbound
                                    .receive()
                                    .asString()
                                    .doOnNext(message ->
                                            processMessage(
                                                    message,
                                                    outbound,
                                                    tickConsumer
                                            )
                                    )
                                    .then();

                    return authentication.and(receiving);
                })
                .subscribe(
                        success -> {
                            System.out.println(
                                    "WebSocket connection completed"
                            );

                            reconnect(
                                    tickConsumer,
                                    attempt
                            );
                        },
                        error -> {
                            System.err.println(
                                    "WebSocket connection error: "
                                            + error.getMessage()
                            );

                            reconnect(
                                    tickConsumer,
                                    attempt
                            );
                        }
                );
    }

    private void reconnect(
            Consumer<MarketTick> tickConsumer,
            int attempt) {

        int delaySeconds =
                Math.min(
                        30,
                        (int) Math.pow(2, Math.min(attempt, 5))
                );

        System.out.println(
                "Reconnecting in "
                        + delaySeconds
                        + " seconds..."
        );

        reactor.core.scheduler.Schedulers
                .boundedElastic()
                .schedule(
                        () -> connectWithRetry(
                                tickConsumer,
                                attempt + 1
                        ),
                        delaySeconds,
                        java.util.concurrent.TimeUnit.SECONDS
                );
    }
    /*
     * Create authentication JSON.
     */
    private String createAuthMessage() {

        return """
                {
                  "action":"auth",
                  "api_key":"%s",
                  "secret":"%s"
                }
                """.formatted(
                apiKey,
                secretKey
        );
    }

    /*
     * Create subscription JSON.
     */
    private String createSubscriptionMessage() {

        try {

            return objectMapper.writeValueAsString(
                    new SubscriptionRequest(
                            "subscribeMarketPrice",
                            symbols
                    )
            );

        } catch (Exception e) {

            throw new RuntimeException(
                    "Unable to create subscription message",
                    e
            );
        }
    }

    /*
     * Process messages received from upstream.
     */
    private void processMessage(
            String message,
            reactor.netty.NettyOutbound outbound,
            Consumer<MarketTick> tickConsumer) {

        try {

            JsonNode json =
                    objectMapper.readTree(message);

            /*
             * Check authentication response.
             */
            if (isAuthenticationSuccessful(json)) {

                System.out.println(
                        "Authentication successful"
                );

                /*
                 * Subscribe only after authentication.
                 */
                outbound
                        .sendString(
                                Mono.just(
                                        createSubscriptionMessage()
                                )
                        )
                        .then()
                        .subscribe();

                return;
            }

            /*
             * Ignore messages that are not market ticks.
             */
            if (!json.has("symbol")) {
                return;
            }

            /*
             * Convert JSON into MarketTick.
             */
            MarketTick tick =
                    objectMapper.treeToValue(
                            json,
                            MarketTick.class
                    );

            /*
             * Send tick to calculation service.
             */
            tickConsumer.accept(tick);

        } catch (Exception e) {

            System.err.println(
                    "Error processing market message: "
                            + e.getMessage()
            );
        }
    }

    /*
     * Detect successful authentication response.
     */
    private boolean isAuthenticationSuccessful(
            JsonNode json) {

        if (json.has("authenticated")) {

            return json
                    .get("authenticated")
                    .asBoolean();
        }

        if (json.has("success")) {

            return json
                    .get("success")
                    .asBoolean();
        }

        if (json.has("status")) {

            return "success".equalsIgnoreCase(
                    json.get("status").asText()
            );
        }

        return false;
    }

    /*
     * Object used to create subscription JSON.
     */
    private record SubscriptionRequest(
            String action,
            List<String> symbols) {
    }
}