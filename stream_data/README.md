# Real-Time Kafka Stream Module

This folder contains all Kafka producers and consumers responsible for real-time data ingestion, and storage for the Stock Market Trend Analysis project.

Each data source (e.g., stock trades, macroeconomic indicators, sentiment) has:

  -  A **Kafka Producer** to collect or generate data.

  -  A **Kafka Consumer** to process and store data in MinIO as Parquet files.

## Kafka Producers Overview

`producer_stockdata_real`:

- **Source**: Alpaca API (live trades).

- **Tickers**: Loaded dynamically from PostgreSQL (`companies_info`).

- **Output Topic**: `stock_trades`.

- **Frequency**: Real-time (around 1 second per ticker).
  
- **Active only**: Monday–Friday, during US market hours, from 9:30 AM to 4:00 PM (US Eastern Time).

`producer_stockdata_fake`:

- **Source**: Synthetic data generated artificially.

- **Tickers**: Loaded dynamically from PostgreSQL (`companies_info`).

- **Output Topic**: `stock_trades`.

- **Frequency**: Real-time (around 1 second per ticker).

- **Active only**: Outside of US market hours (night, weekends).

`producer_macrodata`:

- **Source**: FRED API (e.g., GDP, CPI, interest rates).

- **Output Topic**: `macrodata`.

- **Frequency**: Every 60 minutes (latest daily values only).

`producer_bluesky`:

- **Source**: Bluesky API — searches posts by: static financial keywords and dynamic keywords from PostgreSQL (e.g., `company_name`, `related_words`).

- **Output Topic**: `bluesky`.

- **Frequency**: Every ~30 seconds, with 2s delay per keyword.

`producer_news`:

- **Source**: Finnhub API — retrieves company-related news.

- **Tickers**: Loaded from PostgreSQL (`companies_info`).

- **Output Topic**: `finnhub`.

- **Frequency**: Every 60 seconds.

## Kafka Consumers Overview

`consumer_stockdata`:

- **Input topic**: `stock_trades`.

- **Validates**:  Only saves data during NYSE trading hours (from 9:30 AM to 4:00 PM, US Eastern Time).

- **Storage**: MinIO → Bucket `stock-data`, partitioned by ticker and date.

`consumer_macrodata`:

- **Input topic**: `macrodata`.

- **Storage**: MinIO → Bucket `macro-data`, partitioned by alias and year.

`consumer_bluesky`:

- **Input topic**: `bluesky`.

- **Storage**: MinIO → Bucket `bluesky-data`, partitioned by date and keyword.

`consumer_news`:

- **Input topic**: `finnhub`.

- **Storage**: MinIO → Bucket `finnhub-data`, partitioned by ticker and date.


## Configuration

All scripts are configured via environment variables from the .env file in the project root. Required variables include:

 -   MinIO (S3): `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`

-  Alpaca: `API_KEY_ALPACA`, `API_SECRET_ALPACA`

 -   Finnhub: `FINNHUB_API_KEY`

-    FRED: `API_KEY_FRED`

 -   PostgreSQL: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

 -   Bluesky: `BLUESKY_IDENTIFIER`, `BLUESKY_PASSWORD`

## Notes

- Make sure the PostgreSQL database is running and the `companies_info` table is populated with active companies.

- The `companies_info` table must include relevant values for ticker, company_name, and related_words. These are used for dynamic keyword extraction.

- MinIO buckets will be automatically created by the consumers if they don't exist.
