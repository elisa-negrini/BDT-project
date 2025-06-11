# Sentiment Analysis Pipeline

This folder contains modules for real-time sentiment analysis on financial text streams (news and social media), using a quantized FinBERT model with ONNX and Apache Flink.

## Components

`sentiment_news`

  -  **Source**: Kafka topic `finnhub`

  -  **Task**: Performs sentiment analysis on financial news (headline + summary)

  -  **Features**: Ticker detection via ticker/company name/keywords in headline and summery (at least one, for the request made to the API)

  -  **Outputs to**: Kafka topic `news_sentiment`

`sentiment_bluesky`

  -  **Source**: Kafka topic `bluesky`

  -  **Task**: Performs sentiment analysis on social posts

  -  **Features**: Ticker detection via ticker/company name/keywords in text or fallback to "GENERAL" if no ticker found

  -  **Outputs to**: Kafka topic `bluesky_sentiment`

`sentiment_consumer`

  -  Consumes from: `news_sentiment` and `bluesky_sentiment`
    
  -  Saves results as partitioned Parquet files to MinIO/S3

## Model

- FinBERT (quantized, ONNX format)

- Tokenizer and model are loaded from the subfolder `quantized_finbert`

## Requirements

 -   Python ≥ 3.8

  -  Apache Flink (PyFlink)

   - Kafka + MinIO/S3

  -  PostgreSQL (for `companies_info`)

   - Dependencies: `onnxruntime`, `transformers`, `psycopg2`, `pandas`, `pyarrow`, `s3fs`, `confluent_kafka`, `numpy`, `scipy`

#### ENVIRONMENT VARIABLES

- MinIO (S3): `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`

- PostgreSQL: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
