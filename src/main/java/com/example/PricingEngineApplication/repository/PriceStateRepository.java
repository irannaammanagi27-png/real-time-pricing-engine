package com.example.PricingEngineApplication.repository;

import com.example.PricingEngineApplication.model.PriceSnapshot;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

@Repository
public class PriceStateRepository {

    private static final String SNAPSHOT_PREFIX =
            "price:snapshot:";

    private static final String HISTORY_PREFIX =
            "price:history:";

    private final ReactiveStringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public PriceStateRepository(
            ReactiveStringRedisTemplate redisTemplate,
            ObjectMapper objectMapper) {

        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /*
     * Save the latest calculated snapshot.
     */
    public Mono<Boolean> save(
            String symbol,
            PriceSnapshot snapshot) {

        try {

            String json =
                    objectMapper.writeValueAsString(snapshot);

            return redisTemplate
                    .opsForValue()
                    .set(
                            SNAPSHOT_PREFIX + symbol,
                            json
                    );

        } catch (Exception e) {

            return Mono.error(e);
        }
    }

    /*
     * Restore the latest snapshot.
     */
    public Mono<PriceSnapshot> find(
            String symbol) {

        return redisTemplate
                .opsForValue()
                .get(
                        SNAPSHOT_PREFIX + symbol
                )
                .flatMap(json -> {

                    try {

                        PriceSnapshot snapshot =
                                objectMapper.readValue(
                                        json,
                                        PriceSnapshot.class
                                );

                        return Mono.just(snapshot);

                    } catch (Exception e) {

                        return Mono.error(e);
                    }
                });
    }

    /*
     * Save rolling price history.
     */
    public Mono<Boolean> saveHistory(
            String symbol,
            List<PriceHistoryPoint> history) {

        try {

            String json =
                    objectMapper.writeValueAsString(
                            history
                    );

            return redisTemplate
                    .opsForValue()
                    .set(
                            HISTORY_PREFIX + symbol,
                            json
                    );

        } catch (Exception e) {

            return Mono.error(e);
        }
    }

    /*
     * Restore rolling price history.
     */
    public Mono<List<PriceHistoryPoint>> findHistory(
            String symbol) {

        return redisTemplate
                .opsForValue()
                .get(
                        HISTORY_PREFIX + symbol
                )
                .defaultIfEmpty("[]")
                .flatMap(json -> {

                    try {

                        List<PriceHistoryPoint> history =
                                objectMapper.readValue(
                                        json,
                                        new TypeReference<
                                                List<PriceHistoryPoint>>() {
                                        }
                                );

                        return Mono.just(history);

                    } catch (Exception e) {

                        return Mono.error(e);
                    }
                });
    }

    /*
     * Represents one historical price point.
     */
    public static class PriceHistoryPoint {

        private long timestamp;
        private double price;

        public PriceHistoryPoint() {
        }

        public PriceHistoryPoint(
                long timestamp,
                double price) {

            this.timestamp = timestamp;
            this.price = price;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }
    }
}