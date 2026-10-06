package com.example.PricingEngineApplication.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class AuthenticationService {

    @Value("${upstream.api-key}")
    private String apiKey;

    @Value("${upstream.secret-key}")
    private String secretKey;

    private final ObjectMapper objectMapper;

    public AuthenticationService(
            ObjectMapper objectMapper) {

        this.objectMapper = objectMapper;
    }

    public String createAuthenticationMessage() {

        return """
                {
                    "action": "auth",
                    "api_key": "%s",
                    "secret": "%s"
                }
                """.formatted(
                apiKey,
                secretKey
        );
    }

    public boolean isAuthenticationSuccessful(
            String message) {

        try {

            JsonNode root =
                    objectMapper.readTree(message);

            /*
             * Different WebSocket providers can use
             * different names for successful auth.
             *
             * This checks common success representations.
             */
            if (root.has("success")) {

                return root
                        .get("success")
                        .asBoolean();
            }

            if (root.has("authenticated")) {

                return root
                        .get("authenticated")
                        .asBoolean();
            }

            if (root.has("status")) {

                return "success".equalsIgnoreCase(
                        root.get("status").asText()
                );
            }

        } catch (Exception e) {

            System.err.println(
                    "Authentication response parsing error: "
                            + e.getMessage()
            );
        }

        return false;
    }
}