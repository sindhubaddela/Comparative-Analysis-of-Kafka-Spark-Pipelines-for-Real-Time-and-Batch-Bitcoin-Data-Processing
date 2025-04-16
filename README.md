# Comparative-Analysis-of-Kafka-Spark-Pipelines-for-Real-Time-and-Batch-Bitcoin-Data-Processing
This project implements and compares two data processing pipelines using Apache Kafka and Apache Spark to analyze real-time Bitcoin (BTC-USD) ticker data sourced from the Coinbase Pro WebSocket API:

1.  **Real-Time Streaming Pipeline:** Processes data with low latency using Spark Structured Streaming, performing windowed aggregations.
2.  **Periodic Batch Pipeline:** Accumulates data in an SQLite database and processes it periodically using Spark Batch, performing global aggregations.

The goal is to analyze key trading metrics (average price, volume, trade counts, market sentiment) using both paradigms and evaluate the trade-offs in terms of latency,processing time, and the nature of insights derived. A Streamlit dashboard is provided to visualize and compare the results.

## 2. Architecture

The system consists of the following components:

1.  **Data Ingestion (Coinbase -> Kafka):**
    *   `producer.py`: Connects to the Coinbase Pro WebSocket feed for BTC-USD, filters for relevant ticker messages, and publishes them to the `raw_prices` Kafka topic.

2.  **Streaming Path (Kafka -> Spark Streaming -> CSV):**
    *   `spark_processor.py`: A Spark Structured Streaming application that reads from the `raw_prices` Kafka topic, parses JSON messages, performs 1-minute tumbling window aggregations (calculating metrics like avg price, volume, buy/sell ratio), handles late data with watermarking, and appends results continuously to CSV files in `data/stream_csv/`.

3.  **Batch Path (Kafka -> SQLite -> Spark Batch -> CSV):**
    *   `consumer.py`: A simple Kafka consumer that reads raw messages from the `raw_prices` topic and inserts them into an SQLite database (`data/crypto_prices.db`).
    *   `batch_processor.py`: A Spark Batch application that connects to the SQLite database via JDBC, reads the *entire* `btc_prices` table, performs global aggregations across all historical data loaded, and saves the single-row result to a new CSV file in `data/batch_output/`.
    *   `batch_scheduler.py`: A simple Python script that triggers the `batch_processor.py` Spark job at regular intervals (e.g., every 10 minutes) and logs execution time to `data/batch_log.txt`.

4.  **Visualization (CSV -> Streamlit):**
    *   `dashboard.py`: A Streamlit application that reads the latest aggregated data from both the streaming (`data/stream_csv/`) and batch (`data/batch_output/`) output directories, displaying time-series charts, latest metrics, and performance comparisons.

```mermaid
graph LR
    A[Coinbase Pro WebSocket Feed] --> B(producer.py);
    B --> C{Kafka Topic: raw_prices};

    subgraph Streaming Pipeline
        C --> D(spark_processor.py - Spark Structured Streaming);
        D -- 1-min Window Aggregates --> E[CSV Files: data/stream_csv/];
    end

    subgraph Batch Pipeline
        C --> F(consumer.py);
        F --> G[(SQLite DB: data/crypto_prices.db)];
        G -- Reads All Data via JDBC --> H(batch_processor.py - Spark Batch);
        I(batch_scheduler.py) -- Triggers Periodically --> H;
        H -- Global Aggregates --> J[CSV Files: data/batch_output/];
        I -- Logs Execution Time --> K[Log File: data/batch_log.txt];
    end

    subgraph Visualization
        E --> L(dashboard.py - Streamlit);
        J --> L;
    end

    style G fill:#f9f,stroke:#333,stroke-width:2px
