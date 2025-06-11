# Real-Time Stock Data Aggregation Pipeline

This folder contains three PyFlink jobs responsible for ingesting, aggregating, and enriching real-time financial and sentiment data. Each job plays a specific role in the data pipeline that collects, processes, and merges stock trade data, macroeconomic indicators, and social media sentiment.

## Jobs Overview

`main_job` processes ticker-specific data:

  -  Aggregates prices, sizes, and sentiment over sliding windows (1/5/30min)

  -  Fetches fundamentals from MinIO (EPS, FCF, margins, etc.)

  -  Detects anomalies (e.g., price spikes)

  -  Emits predictions to Kafka topic `main_data`

  -  Sends alerts to `anomaly_detection`

`global_job` processes global data (non-ticker-specific):

  -  Tracks macro indicators (GDP, CPI, FFR, etc.)

  -  Aggregates general sentiment (e.g., from "GENERAL" Bluesky posts)

  -  Emits global context updates to global_data topic

`aggregates_job` joins ticker-level data with global context:

  -  Waits for both data sources

  -  Merges into one record using a fixed schema

  -  Emits to `aggregated_data` topic with partitioning

  -  Creates the topic if it doesn't exist

## Requirements

- Python 3.8+, PyFlink, Kafka

-   PostgreSQL (tickers)

 -   MinIO (fundamentals)

 -   Required Python libs: `kafka-python`, `pyflink`, `minio`, `psycopg2`, `pytz`, `numpy`, `pandas`, `dateutil`

#### ENVIRONMENTAL VARIABLES
