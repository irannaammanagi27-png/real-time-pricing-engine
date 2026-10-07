package com.example.PricingEngineApplication.client;

import com.example.PricingEngineApplication.model.MarketTick;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Component
public class MarketDataWebSocketClient {

    @Value("${upstream.websocket.url}")
    private String websocketUrl;

    @Value("${upstream.api-key}")
    private String apiKey;

    @Value("${upstream.secret-key}")
    private String secretKey;

    private final ObjectMapper objectMapper;

    private final ReactorNettyWebSocketClient webSocketClient;

    private final AtomicBoolean connected =
            new AtomicBoolean(false);

    private Consumer<MarketTick> tickConsumer;

    /*
     * Symbols required by the assignment.
     */
    private final List<String> symbols = Arrays.asList(

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

    public MarketDataWebSocketClient(
            ObjectMapper objectMapper) {

        this.objectMapper = objectMapper;

        this.webSocketClient =
                new ReactorNettyWebSocketClient();
    }

    /*
     * Called by MarketDataService.
     */
    public void connect(
            Consumer<MarketTick> tickConsumer) {

        this.tickConsumer = tickConsumer;

        connectWithRetry(0);
    }

    /*
     * Connect to upstream WebSocket.
     */
    private void connectWithRetry(int attempt) {

        if (connected.get()) {

            System.out.println(
                    "Already connected to market WebSocket"
            );

            return;
        }

        System.out.println(
                "Connecting to market WebSocket..."
        );

        System.out.println(
                "URL: " + websocketUrl
        );

        webSocketClient
                .execute(
                        URI.create(websocketUrl),
                        this::handleConnection
                )

                .doOnSubscribe(subscription ->
                        System.out.println(
                                "Opening upstream WebSocket connection..."
                        )
                )

                .doOnError(error -> {

                    connected.set(false);

                    System.err.println(
                            "Market WebSocket error: "
                                    + error.getMessage()
                    );
                })

                .doFinally(signal -> {

                    connected.set(false);

                    System.out.println(
                            "Market WebSocket closed."
                    );

                    scheduleReconnect(attempt);
                })

                .subscribe();
    }

    /*
     * Handle connection.
     */
    private Mono<Void> handleConnection(
            WebSocketSession session) {

        connected.set(true);

        System.out.println(
                "Connected to market WebSocket"
        );

        /*
         * Create authentication JSON.
         */
        String authMessage;

        try {

            authMessage =
                    objectMapper.writeValueAsString(
                            new AuthRequest(
                                    "auth",
                                    apiKey,
                                    secretKey
                            )
                    );

        } catch (Exception e) {

            return Mono.error(e);
        }

        System.out.println(
                "Sending authentication request..."
        );

        /*
         * Send authentication first.
         *
         * Then continuously receive messages.
         */
        return session
                .send(
                        Mono.just(
                                session.textMessage(
                                        authMessage
                                )
                        )
                )

                .thenMany(

                        session.receive()

                                .map(
                                        WebSocketMessage::getPayloadAsText
                                )

                                .doOnNext(message -> {

                                    /*
                                     * Process the message.
                                     *
                                     * We intentionally do not print
                                     * every market tick to the console.
                                     */
                                    handleUpstreamMessage(
                                            session,
                                            message
                                    );
                                })
                )

                .then();
    }

    /*
     * Process messages from upstream.
     */
    private void handleUpstreamMessage(
            WebSocketSession session,
            String message) {

        try {

            JsonNode root =
                    objectMapper.readTree(message);

            /*
             * Authentication successful.
             */
            if (isAuthenticationSuccess(root)) {

                System.out.println(
                        "Authentication successful"
                );

                sendSubscription(session);

                return;
            }

            /*
             * Authentication failure.
             */
            if (isAuthenticationFailure(root)) {

                System.err.println(
                        "Authentication failed: "
                                + root.path("message").asText(
                                "Unknown authentication error"
                        )
                );

                return;
            }

            /*
             * Market price update.
             */
            if ("marketPriceUpdate".equalsIgnoreCase(
                    root.path("event").asText())) {

                MarketTick tick =
                        parseMarketTick(root);

                if (tick != null) {

                    /*
                     * Send tick to MarketDataService.
                     *
                     * No console printing here because
                     * market updates can arrive very frequently.
                     */
                    if (tickConsumer != null) {

                        tickConsumer.accept(tick);
                    }
                }
            }

        } catch (Exception e) {

            System.err.println(
                    "Error processing upstream message: "
                            + e.getMessage()
            );
        }
    }

    /*
     * Check authentication success.
     */
    private boolean isAuthenticationSuccess(
            JsonNode root) {

        String event =
                root.path("event").asText("");

        String status =
                root.path("status").asText("");

        String message =
                root.path("message").asText("");

        return event.equalsIgnoreCase("authSuccess")

                || event.equalsIgnoreCase("authenticated")

                || status.equalsIgnoreCase("success")

                || message.toLowerCase()
                .contains("authentication successful")

                || message.toLowerCase()
                .contains("authenticated");
    }

    /*
     * Check authentication failure.
     */
    private boolean isAuthenticationFailure(
            JsonNode root) {

        String event =
                root.path("event").asText("");

        String status =
                root.path("status").asText("");

        String message =
                root.path("message").asText("");

        return event.equalsIgnoreCase("authFailed")

                || event.equalsIgnoreCase(
                "authenticationFailed")

                || status.equalsIgnoreCase("error")

                || status.equalsIgnoreCase("failed")

                || message.toLowerCase()
                .contains("authentication failed")

                || message.toLowerCase()
                .contains("active session");
    }

    /*
     * Subscribe to market prices.
     */
    private void sendSubscription(
            WebSocketSession session) {

        try {

            String subscriptionMessage =
                    objectMapper.writeValueAsString(

                            new SubscribeRequest(
                                    "subscribeMarketPrice",
                                    symbols
                            )
                    );

            System.out.println(
                    "Sending market price subscription..."
            );

            System.out.println(
                    "Number of symbols: "
                            + symbols.size()
            );

            session.send(

                    Mono.just(
                            session.textMessage(
                                    subscriptionMessage
                            )
                    )

            ).subscribe(

                    unused -> {
                    },

                    error ->
                            System.err.println(
                                    "Subscription error: "
                                            + error.getMessage()
                            ),

                    () ->
                            System.out.println(
                                    "Market price subscription sent."
                            )
            );

        } catch (Exception e) {

            System.err.println(
                    "Unable to create subscription: "
                            + e.getMessage()
            );
        }
    }

    /*
     * Convert JSON into MarketTick.
     */
    private MarketTick parseMarketTick(
            JsonNode root) {

        String symbol =
                root.path("symbol").asText(null);

        if (symbol == null
                || symbol.isBlank()) {

            return null;
        }

        double bid =
                root.path("bid").asDouble();

        double ask =
                root.path("ask").asDouble();

        /*
         * Ignore invalid prices.
         */
        if (bid <= 0 || ask <= 0) {

            System.err.println(
                    "Invalid price received for "
                            + symbol
            );

            return null;
        }

        MarketTick tick =
                new MarketTick();

        tick.setSymbol(
                symbol.toUpperCase()
        );

        /*
         * Upstream bid = buy price.
         */
        tick.setBuy(bid);

        /*
         * Upstream ask = sell price.
         */
        tick.setSell(ask);

        /*
         * Do not use upstream 24h values.
         *
         * PriceCalculationService calculates
         * these values.
         */
        tick.setHigh24h(0);

        tick.setLow24h(0);

        tick.setChange24h(0);

        /*
         * Use local receipt timestamp.
         */
        tick.setTimestamp(
                System.currentTimeMillis()
        );

        return tick;
    }

    /*
     * Reconnect with exponential backoff.
     */
    private void scheduleReconnect(
            int attempt) {

        int nextAttempt =
                Math.min(
                        attempt + 1,
                        5
                );

        long delaySeconds =
                Math.min(
                        (long) Math.pow(2, attempt),
                        30
                );

        System.out.println(
                "Reconnecting in "
                        + delaySeconds
                        + " seconds..."
        );

        Mono.delay(
                Duration.ofSeconds(
                        delaySeconds
                )
        )
        .subscribe(
                ignored ->
                        connectWithRetry(
                                nextAttempt
                        )
        );
    }

    /*
     * Return all symbols.
     */
    public List<String> getSymbols() {

        return symbols;
    }

    /*
     * Authentication request.
     */
    private record AuthRequest(
            String action,
            String api_key,
            String secret) {
    }

    /*
     * Subscription request.
     */
    private record SubscribeRequest(
            String action,
            List<String> symbols) {
    }
}