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

    private static class PricePoint {

        private final long timestamp;
        private final double price;

        private PricePoint(long timestamp, double price) {
            this.timestamp = timestamp;
            this.price = price;
        }
    }

    /*
     * Stores all price points for each symbol
     * for the rolling 24-hour window.
     */
    private final Map<String, Deque<PricePoint>> priceHistory =
            new ConcurrentHashMap<>();

    /*
     * Monotonic deque used to calculate 24-hour high efficiently.
     */
    private final Map<String, Deque<PricePoint>> maxHistory =
            new ConcurrentHashMap<>();

    /*
     * Monotonic deque used to calculate 24-hour low efficiently.
     */
    private final Map<String, Deque<PricePoint>> minHistory =
            new ConcurrentHashMap<>();

    /*
     * Stores the latest processed timestamp for each symbol.
     */
    private final Map<String, Long> latestTimestamp =
            new ConcurrentHashMap<>();

    /*
     * Stores the latest calculated snapshot for each symbol.
     *
     * OrderService will use this map to get the
     * current market price for BUY/SELL orders.
     */
    private final Map<String, PriceSnapshot> latestSnapshots =
            new ConcurrentHashMap<>();

    /*
     * Keeps track of symbols whose Redis state has been restored.
     */
    private final Map<String, Boolean> restoredSymbols =
            new ConcurrentHashMap<>();

    private final PriceStateRepository repository;

    public PriceCalculationService(
            PriceStateRepository repository) {

        this.repository = repository;
    }

    /*
     * Restore 24-hour price history from Redis.
     */
    public Mono<Void> restoreSymbol(String symbol) {

        return repository
                .findHistory(symbol)
                .doOnNext(history -> {

                    if (history == null || history.isEmpty()) {

                        restoredSymbols.put(
                                symbol,
                                true
                        );

                        return;
                    }

                    synchronized (this) {

                        Deque<PricePoint> priceDeque =
                                priceHistory.computeIfAbsent(
                                        symbol,
                                        key -> new ArrayDeque<>()
                                );

                        Deque<PricePoint> maxDeque =
                                maxHistory.computeIfAbsent(
                                        symbol,
                                        key -> new ArrayDeque<>()
                                );

                        Deque<PricePoint> minDeque =
                                minHistory.computeIfAbsent(
                                        symbol,
                                        key -> new ArrayDeque<>()
                                );

                        priceDeque.clear();
                        maxDeque.clear();
                        minDeque.clear();

                        latestTimestamp.remove(symbol);

                        for (PriceStateRepository.PriceHistoryPoint point
                                : history) {

                            PricePoint pricePoint =
                                    new PricePoint(
                                            point.getTimestamp(),
                                            point.getPrice()
                                    );

                            priceDeque.addLast(
                                    pricePoint
                            );

                            /*
                             * Build maximum monotonic deque.
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
                             * Build minimum monotonic deque.
                             */
                            while (!minDeque.isEmpty()
                                    && minDeque.peekLast().price
                                    >= pricePoint.price) {

                                minDeque.removeLast();
                            }

                            minDeque.addLast(
                                    pricePoint
                            );

                            Long latest =
                                    latestTimestamp.get(symbol);

                            if (latest == null
                                    || pricePoint.timestamp > latest) {

                                latestTimestamp.put(
                                        symbol,
                                        pricePoint.timestamp
                                );
                            }
                        }
                    }

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
     * Process one incoming market tick.
     */
    public synchronized PriceSnapshot process(
            MarketTick tick) {

        if (tick == null) {
            return null;
        }

        String symbol = tick.getSymbol();

        /*
         * Validate symbol.
         */
        if (symbol == null || symbol.isBlank()) {
            return null;
        }

        /*
         * Upstream does not provide a timestamp,
         * so use the local receipt time.
         */
        long timestamp = tick.getTimestamp();

        if (timestamp <= 0) {

            timestamp =
                    System.currentTimeMillis();

            tick.setTimestamp(timestamp);
        }

        /*
         * Get the latest processed timestamp.
         */
        Long previousTimestamp =
                latestTimestamp.get(symbol);

        /*
         * Reject out-of-order tick.
         */
        if (previousTimestamp != null
                && timestamp < previousTimestamp) {

            return null;
        }

        /*
         * Reject duplicate tick.
         */
        if (previousTimestamp != null
                && timestamp == previousTimestamp) {

            return null;
        }

        /*
         * Calculate mid price.
         *
         * mid = (bid + ask) / 2
         */
        double midPrice =
                (tick.getBuy() + tick.getSell())
                        / 2.0;

        /*
         * Get/create history structures.
         */
        Deque<PricePoint> history =
                priceHistory.computeIfAbsent(
                        symbol,
                        key -> new ArrayDeque<>()
                );

        Deque<PricePoint> maxDeque =
                maxHistory.computeIfAbsent(
                        symbol,
                        key -> new ArrayDeque<>()
                );

        Deque<PricePoint> minDeque =
                minHistory.computeIfAbsent(
                        symbol,
                        key -> new ArrayDeque<>()
                );

        PricePoint currentPoint =
                new PricePoint(
                        timestamp,
                        midPrice
                );

        /*
         * Add current price to normal history.
         */
        history.addLast(
                currentPoint
        );

        /*
         * Maintain maximum monotonic deque.
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
         * Maintain minimum monotonic deque.
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
                timestamp - TWENTY_FOUR_HOURS;

        /*
         * Remove prices older than 24 hours.
         */
        while (!history.isEmpty()
                && history.peekFirst().timestamp
                < minimumTimestamp) {

            history.removeFirst();
        }

        /*
         * Remove expired maximum candidates.
         */
        while (!maxDeque.isEmpty()
                && maxDeque.peekFirst().timestamp
                < minimumTimestamp) {

            maxDeque.removeFirst();
        }

        /*
         * Remove expired minimum candidates.
         */
        while (!minDeque.isEmpty()
                && minDeque.peekFirst().timestamp
                < minimumTimestamp) {

            minDeque.removeFirst();
        }

        /*
         * The first element of maxDeque
         * is the current 24-hour high.
         */
        double high24h =
                maxDeque.isEmpty()
                        ? midPrice
                        : maxDeque.peekFirst().price;

        /*
         * The first element of minDeque
         * is the current 24-hour low.
         */
        double low24h =
                minDeque.isEmpty()
                        ? midPrice
                        : minDeque.peekFirst().price;

        /*
         * Oldest price currently inside
         * the 24-hour window.
         */
        double oldPrice =
                history.isEmpty()
                        ? midPrice
                        : history.peekFirst().price;

        /*
         * Calculate 24-hour percentage change.
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
         * Create final price snapshot.
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
         * IMPORTANT:
         *
         * Store latest snapshot in memory.
         *
         * OrderService will use this when
         * placing BUY/SELL market orders.
         */
        latestSnapshots.put(
                symbol,
                snapshot
        );

        /*
         * Save latest snapshot to Redis.
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
         * Save rolling 24-hour history to Redis.
         */
        saveHistory(
                symbol,
                history
        );

        return snapshot;
    }

    /*
     * Get the latest calculated snapshot
     * for a particular symbol.
     *
     * This method is used by OrderService.
     */
    public PriceSnapshot getLatestSnapshot(
            String symbol) {

        if (symbol == null
                || symbol.isBlank()) {

            return null;
        }

        return latestSnapshots.get(
                symbol
        );
    }

    /*
     * Save price history to Redis.
     */
    private void saveHistory(
            String symbol,
            Deque<PricePoint> history) {

        List<PriceStateRepository.PriceHistoryPoint>
                redisHistory =
                new ArrayList<>();

        for (PricePoint point : history) {

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