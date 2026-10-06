package com.example.PricingEngineApplication.service;


import org.springframework.stereotype.Service;

import com.example.PricingEngineApplication.model.PriceSnapshot;
import com.example.PricingEngineApplication.repository.PriceStateRepository;

import reactor.core.publisher.Mono;

@Service
public class StatePersistenceService {

    private final PriceStateRepository repository;

    public StatePersistenceService(
            PriceStateRepository repository) {

        this.repository = repository;
    }

    public Mono<Boolean> save(
            PriceSnapshot snapshot) {

        return repository.save(
                snapshot.getSymbol(),
                snapshot
        );
    }

    public Mono<PriceSnapshot> restore(
            String symbol) {

        return repository.find(symbol);
    }
}