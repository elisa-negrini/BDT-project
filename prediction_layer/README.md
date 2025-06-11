# Prediction Layer

This folder contains the modules responsible for the **real-time prediction** of future stock prices using pre-trained LSTM models, and the **persistence** of predictions to MinIO (or any S3-compatible storage).

## Contents

`prediction_layer.py`:

  A Kafka consumer that listens to the `aggregated_data` topic, loads ticker-specific LSTM models and scalers, and generates minute-level price predictions. Each prediction is published to the `prediction` Kafka topic. The system:
- Buffers incoming feature vectors per ticker
- Avoids predictions during market warm-up
- Uses a separate model and scaler for each ticker
- Optionally emphasizes a key feature (e.g., `price_mean_1min`)
- Sends output as JSON to Kafka for downstream usage

`consumer_prediction.py`:

A Kafka consumer that listens to the `prediction` topic and saves each real (non-simulated) prediction to MinIO/S3 in Parquet format. It:
- Verifies or creates the target bucket (`prediction`)
- Converts each message to a DataFrame
- Partitions files by `ticker` and `month` (e.g., `ticker=AAPL/month=25-06/`)
- Uses `pyarrow` and `s3fs` to write Parquet files directly to S3-compatible storage

## Requirements

- Python ≥ 3.8
- Kafka running at `kafka:9092`
- A configured MinIO/S3 instance
- Python packages:
  - `tensorflow`, `numpy`, `pandas`, `joblib`, `confluent_kafka`
  - `s3fs`, `boto3`, `pyarrow`, `scikit-learn`, `pytz`
