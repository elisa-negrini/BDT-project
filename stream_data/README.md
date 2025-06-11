# Real-Time Stream Data Module

This folder contains all Kafka producers and consumers responsible for real-time data ingestion, and storage for the Stock Market Trend Analysis project.

Each data source (e.g., stock trades, macroeconomic indicators, sentiment) has:

  -  A **Kafka Producer** to collect or generate data

  -  A **Kafka Consumer** to process and store data in MinIO as Parquet files


## Producers Overview

producer_stockdata:

- Streams real-time trade data using Alpaca API.

- Falls back to synthetic data outside market hours.

- Tickers are loaded dynamically from PostgreSQL.

- Sends to topic: stock_trades.

producer_macrodata:

- Periodically fetches data from FRED.

- Sends the latest observations for indicators like GDP, CPI, T10Y, etc.

- Sends to topic: macrodata.

producer_bluesky:

- Authenticates with the Bluesky API.

- Uses keywords from both static financial terms and the companies_info table.

- Sends matched posts to topic: bluesky.