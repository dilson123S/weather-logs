## Esquema visual - Arquitectura

Diagrama de alto nivel del sistema (Mermaid):

```mermaid
flowchart LR
  Producer[Productor (Python)\nweather_producer]
  Rabbit["RabbitMQ\nweather_rabbitmq"]
  Consumer1[Consumidor 1\nweather-logs-consumer-1]
  Consumer2[Consumidor 2\nweather-logs-consumer-2]
  Postgres[(PostgreSQL\nweather_postgres)]
  Prom[Prometheus]
  Graf[Grafana]

  Producer -->|publish weather.*| Rabbit
  Rabbit -->|queue: weather.logs| Consumer1
  Rabbit -->|queue: weather.logs| Consumer2
  Consumer1 -->|inserts| Postgres
  Consumer2 -->|inserts| Postgres
  Prom -->|scrape metrics| Rabbit
  Prom -->|scrape metrics| Consumer1
  Graf -->|datasource: Prometheus| Prom

  subgraph Network [Docker network: weather_net]
    Producer
    Rabbit
    Consumer1
    Consumer2
    Postgres
    Prom
    Graf
  end

```

Descripción: el productor publica mensajes por tópico en `weather.exchange`; RabbitMQ enruta a la cola `weather.logs` y los consumidores procesan y persisten en `PostgreSQL`. Prometheus recolecta métricas y Grafana las visualiza.
