# Machine Learning model

This folder contains the two configurations to train and retrain the model on both historical and new incoming stream aggregated data. 

## Folders Overview

`retrain`:

- **Source**: Live aggregated data stock.

- **Output**: Different model for each ticker

- **Retraining logic**: After the retraining has been completed the new models are mounted on the prediction layer volumes 

`train`:

- **Source**: Historical aggregated data stock stored in a postegres table. 

- **Output **: Different model for each ticker

- **Volume**: 12 millions historical 

- **Train logic**: Once aggregated data are sent to a kafka topic and then u

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

 Environment variables from .env used by the models:

- PostgreSQL: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

