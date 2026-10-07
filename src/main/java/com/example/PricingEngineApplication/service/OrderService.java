package com.example.PricingEngineApplication.service;

import com.example.PricingEngineApplication.model.Order;
import com.example.PricingEngineApplication.model.OrderRequest;
import com.example.PricingEngineApplication.model.OrderResponse;
import com.example.PricingEngineApplication.model.PriceSnapshot;
import com.example.PricingEngineApplication.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    private final PriceCalculationService priceCalculationService;

    public OrderService(
            OrderRepository orderRepository,
            PriceCalculationService priceCalculationService) {

        this.orderRepository = orderRepository;
        this.priceCalculationService = priceCalculationService;
    }

    public OrderResponse placeOrder(OrderRequest request) {

        // Validate request
        if (request == null) {

            throw new RuntimeException(
                    "Order request is required");
        }

        // Validate symbol
        if (request.getSymbol() == null
                || request.getSymbol().isBlank()) {

            throw new RuntimeException(
                    "Symbol is required");
        }

        // Validate quantity
        if (request.getQuantity() <= 0) {

            throw new RuntimeException(
                    "Quantity must be greater than zero");
        }

        // Validate BUY / SELL
        if (request.getSide() == null
                || (!request.getSide().equalsIgnoreCase("BUY")
                && !request.getSide().equalsIgnoreCase("SELL"))) {

            throw new RuntimeException(
                    "Side must be BUY or SELL");
        }

        /*
         * Get the latest live price from
         * PriceCalculationService.
         */
        PriceSnapshot snapshot =
                priceCalculationService.getLatestSnapshot(
                        request.getSymbol()
                );

        // Make sure price is available
        if (snapshot == null) {

            throw new RuntimeException(
                    "Price not available for "
                            + request.getSymbol());
        }

        /*
         * Market BUY executes at ASK.
         * Market SELL executes at BID.
         */
        double executionPrice;

        if (request.getSide().equalsIgnoreCase("BUY")) {

            executionPrice = snapshot.getSell();

        } else {

            executionPrice = snapshot.getBuy();
        }

        /*
         * Create database order.
         */
        Order order = new Order();

        order.setOrderId(
                "ORD-"
                        + UUID.randomUUID()
                        .toString()
                        .substring(0, 8)
        );

        order.setSymbol(
                request.getSymbol().toUpperCase()
        );

        order.setSide(
                request.getSide().toUpperCase()
        );

        order.setQuantity(
                request.getQuantity()
        );

        order.setExecutionPrice(
                executionPrice
        );

        order.setStatus(
                "EXECUTED"
        );

        order.setCreatedAt(
                LocalDateTime.now()
        );

        /*
         * Save order to MySQL.
         */
        Order savedOrder =
                orderRepository.save(order);

        /*
         * Create response.
         */
        OrderResponse response =
                new OrderResponse();

        response.setOrderId(
                savedOrder.getOrderId()
        );

        response.setSymbol(
                savedOrder.getSymbol()
        );

        response.setSide(
                savedOrder.getSide()
        );

        response.setQuantity(
                savedOrder.getQuantity()
        );

        response.setExecutionPrice(
                savedOrder.getExecutionPrice()
        );

        response.setStatus(
                savedOrder.getStatus()
        );

        return response;
    }
}