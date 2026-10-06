package com.example.PricingEngineApplication.service;

import com.example.PricingEngineApplication.model.MarketTick;
import com.example.PricingEngineApplication.model.PriceSnapshot;
import com.example.PricingEngineApplication.repository.PriceStateRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PriceCalculationService {

    private static final long TWENTY_FOUR_HOURS =
            24 * 60 * 60 * 1000L;

    /*
     * Represents one price point in our
     * rolling 24-hour history.
     */
    private static class PricePoint {

        private final long timestamp;
        private final double price;

        private PricePoint(
                long timestamp,
                double price) {

            this.timestamp = timestamp;
            this.price = price;
        }
    }

    /*
     * Normal chronological history.
     *
     * Used for calculating the 24-hour
     * percentage change.
     */
    private final Map<String, Deque<PricePoint>>
            priceHistory =
            new ConcurrentHashMap<>();

    /*
     * Monotonic decreasing deque.
     *
     * First element is always the
     * highest price in the window.
     */
    private final Map<String, Deque<PricePoint>>
            maxHistory =
            new ConcurrentHashMap<>();

    /*
     * Monotonic increasing deque.
     *
     * First element is always the
     * lowest price in the window.
     */
    private final Map<String, Deque<PricePoint>>
            minHistory =
            new ConcurrentHashMap<>();

    /*
     * Latest processed timestamp for
     * each symbol.
     */
    private final Map<String, Long>
            latestTimestamp =
            new ConcurrentHashMap<>();

    /*
     * Tracks symbols for which Redis
     * restoration has completed.
     */
    private final Map<String, Boolean>
            restoredSymbols =
            new ConcurrentHashMap<>();

    private final PriceStateRepository repository;

    public PriceCalculationService(
            PriceStateRepository repository) {

        this.repository = repository;
    }

    /*
     * Restore one symbol's history from Redis.
     *
     * This method is called during application
     * startup BEFORE the upstream WebSocket
     * begins processing live ticks.
     */
    public Mono<Void> restoreSymbol(
            String symbol) {

        return repository
                .findHistory(symbol)

                .doOnNext(history -> {

                    /*
                     * No previous history.
                     */
                    if (history == null
                            || history.isEmpty()) {

                        restoredSymbols.put(
                                symbol,
                                true
                        );

                        return;
                    }

                    synchronized (this) {

                        Deque<PricePoint>
                                priceDeque =
                                priceHistory
                                        .computeIfAbsent(
                                                symbol,
                                                key ->
                                                        new ArrayDeque<>()
                                        );

                        Deque<PricePoint>
                                maxDeque =
                                maxHistory
                                        .computeIfAbsent(
                                                symbol,
                                                key ->
                                                        new ArrayDeque<>()
                                        );

                        Deque<PricePoint>
                                minDeque =
                                minHistory
                                        .computeIfAbsent(
                                                symbol,
                                                key ->
                                                        new ArrayDeque<>()
                                        );

                        /*
                         * Clear any existing in-memory state.
                         */
                        priceDeque.clear();
                        maxDeque.clear();
                        minDeque.clear();

                        latestTimestamp.remove(
                                symbol
                        );

                        /*
                         * Rebuild all in-memory
                         * data structures.
                         */
                        for (
                                PriceStateRepository.PriceHistoryPoint point
                                : history) {

                            PricePoint pricePoint =
                                    new PricePoint(
                                            point.getTimestamp(),
                                            point.getPrice()
                                    );

                            /*
                             * Normal history.
                             */
                            priceDeque.addLast(
                                    pricePoint
                            );

                            /*
                             * Rebuild maximum deque.
                             */
                            while (!maxDeque.isEmpty()
                                    && maxDeque.peekLast().price
                                    <= pricePoint.price) {

                                maxDeque.removeLast();
                            }

                            maxDeque.addLast(
                                    pricePoint
                            );

                            /*
                             * Rebuild minimum deque.
                             */
                            while (!minDeque.isEmpty()
                                    && minDeque.peekLast().price
                                    >= pricePoint.price) {

                                minDeque.removeLast();
                            }

                            minDeque.addLast(
                                    pricePoint
                            );

                            /*
                             * Find latest timestamp.
                             */
                            Long latest =
                                    latestTimestamp.get(
                                            symbol
                                    );

                            if (latest == null
                                    || pricePoint.timestamp
                                    > latest) {

                                latestTimestamp.put(
                                        symbol,
                                        pricePoint.timestamp
                                );
                            }
                        }
                    }

                    /*
                     * Mark restoration as complete.
                     */
                    restoredSymbols.put(
                            symbol,
                            true
                    );

                    System.out.println(
                            "Restored price history for "
                                    + symbol
                    );
                })

                .then();
    }

    /*
     * Process one live market tick.
     */
    public synchronized PriceSnapshot process(
            MarketTick tick) {

        if (tick == null) {
            return null;
        }

        String symbol =
                tick.getSymbol();

        /*
         * Validate symbol.
         */
        if (symbol == null
                || symbol.isBlank()) {

            return null;
        }

        /*
         * A timestamp is not provided by the
         * upstream message, so use local
         * receipt time.
         */
        long timestamp =
                tick.getTimestamp();

        if (timestamp <= 0) {

            timestamp =
                    System.currentTimeMillis();

            tick.setTimestamp(
                    timestamp
            );
        }

        /*
         * Get previous timestamp.
         */
        Long previousTimestamp =
                latestTimestamp.get(
                        symbol
                );

        /*
         * Ignore out-of-order tick.
         */
        if (previousTimestamp != null
                && timestamp < previousTimestamp) {

            return null;
        }

        /*
         * Ignore duplicate tick timestamp.
         */
        if (previousTimestamp != null
                && timestamp == previousTimestamp) {

            return null;
        }

        /*
         * Calculate mid price.
         *
         * mid = (buy + sell) / 2
         */
        double midPrice =
                (tick.getBuy()
                        + tick.getSell())
                        / 2.0;

        /*
         * Get or create history queues.
         */
        Deque<PricePoint> history =
                priceHistory.computeIfAbsent(
                        symbol,
                        key ->
                                new ArrayDeque<>()
                );

        Deque<PricePoint> maxDeque =
                maxHistory.computeIfAbsent(
                        symbol,
                        key ->
                                new ArrayDeque<>()
                );

        Deque<PricePoint> minDeque =
                minHistory.computeIfAbsent(
                        symbol,
                        key ->
                                new ArrayDeque<>()
                );

        /*
         * Create current price point.
         */
        PricePoint currentPoint =
                new PricePoint(
                        timestamp,
                        midPrice
                );

        /*
         * Add to normal history.
         */
        history.addLast(
                currentPoint
        );

        /*
         * Maintain maximum deque.
         *
         * Remove smaller values from the back.
         */
        while (!maxDeque.isEmpty()
                && maxDeque.peekLast().price
                <= midPrice) {

            maxDeque.removeLast();
        }

        maxDeque.addLast(
                currentPoint
        );

        /*
         * Maintain minimum deque.
         *
         * Remove larger values from the back.
         */
        while (!minDeque.isEmpty()
                && minDeque.peekLast().price
                >= midPrice) {

            minDeque.removeLast();
        }

        minDeque.addLast(
                currentPoint
        );

        /*
         * Calculate the beginning of
         * the rolling 24-hour window.
         */
        long minimumTimestamp =
                timestamp
                        - TWENTY_FOUR_HOURS;

        /*
         * Remove old normal history.
         */
        while (!history.isEmpty()
                && history.peekFirst().timestamp
                < minimumTimestamp) {

            history.removeFirst();
        }

        /*
         * Remove old maximum values.
         */
        while (!maxDeque.isEmpty()
                && maxDeque.peekFirst().timestamp
                < minimumTimestamp) {

            maxDeque.removeFirst();
        }

        /*
         * Remove old minimum values.
         */
        while (!minDeque.isEmpty()
                && minDeque.peekFirst().timestamp
                < minimumTimestamp) {

            minDeque.removeFirst();
        }

        /*
         * Highest price in last 24 hours.
         */
        double high24h =
                maxDeque.isEmpty()
                        ? midPrice
                        : maxDeque
                                .peekFirst()
                                .price;

        /*
         * Lowest price in last 24 hours.
         */
        double low24h =
                minDeque.isEmpty()
                        ? midPrice
                        : minDeque
                                .peekFirst()
                                .price;

        /*
         * Oldest price still inside
         * the 24-hour window.
         */
        double oldPrice =
                history.isEmpty()
                        ? midPrice
                        : history
                                .peekFirst()
                                .price;

        /*
         * Calculate percentage change.
         *
         * ((current - old) / old) * 100
         */
        double change24h = 0.0;

        if (oldPrice != 0.0) {

            change24h =
                    ((midPrice - oldPrice)
                            / oldPrice)
                            * 100.0;
        }

        /*
         * Update latest timestamp.
         */
        latestTimestamp.put(
                symbol,
                timestamp
        );

        /*
         * Create output snapshot.
         */
        PriceSnapshot snapshot =
                new PriceSnapshot();

        snapshot.setSymbol(
                symbol
        );

        snapshot.setBuy(
                tick.getBuy()
        );

        snapshot.setSell(
                tick.getSell()
        );

        snapshot.setHigh24h(
                high24h
        );

        snapshot.setLow24h(
                low24h
        );

        snapshot.setChange24h(
                change24h
        );

        snapshot.setTimestamp(
                timestamp
        );

        /*
         * Persist latest snapshot.
         */
        repository
                .save(
                        symbol,
                        snapshot
                )
                .subscribe(
                        success -> {
                        },

                        error ->
                                System.err.println(
                                        "Redis snapshot error: "
                                                + error.getMessage()
                                )
                );

        /*
         * Persist rolling history.
         */
        saveHistory(
                symbol,
                history
        );

        return snapshot;
    }

    /*
     * Save the current 24-hour history
     * to Redis.
     */
    private void saveHistory(
            String symbol,
            Deque<PricePoint> history) {

        List<PriceStateRepository.PriceHistoryPoint>
                redisHistory =
                new ArrayList<>();

        for (
                PricePoint point
                : history) {

            redisHistory.add(
                    new PriceStateRepository.PriceHistoryPoint(
                            point.timestamp,
                            point.price
                    )
            );
        }

        repository
                .saveHistory(
                        symbol,
                        redisHistory
                )
                .subscribe(
                        success -> {
                        },

                        error ->
                                System.err.println(
                                        "Redis history error for "
                                                + symbol
                                                + ": "
                                                + error.getMessage()
                                )
                );
    }
}