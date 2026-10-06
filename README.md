# Real-Time Pricing Engine

A low-latency, fault-tolerant real-time market data processing system
built using Java, Spring Boot, WebSocket and Redis.

## Architecture

```text
                 LiveFXHub
                    |
                    | WebSocket
                    v
        +-------------------------+
        |     Spring Boot         |
        |                         |
        | WebSocket Client        |
        |          |              |
        |          v              |
        | Market Data Processor   |
        |          |              |
        |          v              |
        | Price Calculation       |
        |          |              |
        |          v              |
        |       Redis             |
        |          |              |
        |          v              |
        | WebSocket Server        |
        +----------+--------------+
                   |
          +--------+--------+
          |        |        |
        Client   Client   Client