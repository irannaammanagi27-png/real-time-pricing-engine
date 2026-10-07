package com.example.PricingEngineApplication.controller;

import com.example.PricingEngineApplication.model.OrderRequest;
import com.example.PricingEngineApplication.model.OrderResponse;
import com.example.PricingEngineApplication.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@CrossOrigin(origins = {
        "http://localhost:5173",
        "http://localhost:5174"
})
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> placeOrder(
            @RequestBody OrderRequest request) {

        OrderResponse response =
                orderService.placeOrder(request);

        return ResponseEntity.ok(response);
    }
}