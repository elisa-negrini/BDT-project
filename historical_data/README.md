# Historical Data Ingestion Module

This folder contains all the Kafka producers and consumers responsible for the ingestion, transformation, and storage of historical data used in model training and retraining for the Stock Market Trend Analysis project.

These components set the past data ingestion pipeline and populate the system with backfilled financial, macroeconomic, and sentiment data.

## Kafka Producers Overview

`producer_h_stockdata`:

- **Source**: Historical stock trade data downloaded via Alpaca API.

- **Tickers**: Automatically retrieved from PostgreSQL (`companies_info` table)

- **Output Topic**: `h_alpaca`

- **Frequency**: Sends 1 week of 1-minute data per message per ticker.

- **Features**: Time range: from 2021-01-01 to yesterday (09:30–16:00 ET)

`producer_h_macrodata`:

- **Source**: Daily macroeconomic indicators from FRED API.

- **Variables**: Includes CPI, GDP, unemployment, interest rates, etc.

- **Output Topic**: `historical_macro`

- **Frequency**: One message per macro-variable per file.

`producer_h_company`:

- **Source**: Pre-collected Parquet file of company fundamentals (`df_company_fundamentals.parquet`).

- **Output Topic**: `h_company`

- **Frequency**: Each record includes ticker, calendarYear, and financial metrics.

## Kafka Consumers Overview

`consumer_h_stockdata`:

- **Input Kafka Topic**: `h_alpaca`

- **Storage**: MinIO → Bucket `stock-data/`, organized by ticker/year/month/

- **Format**: Stores raw 1-minute data with timestamps, one Parquet file per (ticker, week)

`consumer_h_macrodata`:

- **Input Kafka Topic**: `h_macrodata`

- **Storage**: MinIO → Bucket `macro-data/`, partitioned by series and year

- **Format**: Each Parquet file contains a single macroeconomic record (series, date, value)

`consumer_h_company`:

- **Input Kafka Topic**: `h_company`

- **Storage**: MinIO → Bucket `company-fundamentals/`, partitioned by symbol and year

- **Format**: One Parquet file per (ticker, calendarYear)

### Historical Data Aggregation module

`historical_aggregated`:

- **Purpose**: It combines raw stock, macroeconomic, and fundamental data, performs necessary feature engineering, and then stores the enriched dataset in PostgreSQL for model training. Upon completion, it sends a signal to trigger the next steps in our MLOps pipeline.

- **Inputs**: Raw data from Kafka topics (`h_alpaca`, `historical_macro`, `h_company`) and active ticker information from PostgreSQL.

- **Output**: A comprehensive, feature-engineered historical dataset in PostgreSQL (`aggregated_data` table), plus a completion signal sent to the start_model Kafka topic.

## Configuration

All producers and consumers use environment variables from .env. Required variables:

- MinIO (S3): `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`

- PostgreSQL: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

- Alpaca: `API_KEY_ALPACA`, `API_SECRET_ALPACA`

- FRED: `API_KEY_FRED`
