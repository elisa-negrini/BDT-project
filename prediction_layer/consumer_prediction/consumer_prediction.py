import os
import json
import time
import pandas as pd
import s3fs
import boto3
from confluent_kafka import Consumer, KafkaError
from botocore.exceptions import ClientError
import pyarrow

# === Configuration ===
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'prediction'
S3_ENDPOINT = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'prediction'

# === Ensure S3 bucket exists ===
def ensure_bucket_exists():
    s3 = boto3.resource(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    try:
        s3.meta.client.head_bucket(Bucket=S3_BUCKET)
    except ClientError:
        print(f"Bucket '{S3_BUCKET}' not found, creating it...")
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' created.")

# === S3 Filesystem for pyarrow ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT}
)

# === Kafka consumer configuration ===
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'prediction_consumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# === Start listening ===
ensure_bucket_exists()
print(f"Listening to topic: {TOPIC}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print(f"Error parsing JSON: {e}")
            continue

        timestamp = data.get("timestamp")
        ticker = data.get("ticker")
        prediction = data.get("prediction")
        is_simulated_prediction = data.get("is_simulated_prediction")

        # Skip simulated predictions
        if is_simulated_prediction:
            print(f"Simulated prediction for {ticker} at {timestamp}, skipping.")
            continue

        if not timestamp or not prediction or not ticker:
            print("Incomplete message, skipping. Full content:")
            print(json.dumps(data, indent=2))
            continue

        try:
            ts = pd.to_datetime(timestamp, utc=True)
        except Exception as e:
            print(f"Timestamp parsing error: {e}, skipping.")
            continue
            
        row = {
            "Ticker": ticker,
            "Timestamp": ts.isoformat(),
            "Prediction": prediction,
        }
        
        df = pd.DataFrame([row])

        month_str = f"{ts.year % 100:02d}-{ts.month:02d}"  # e.g. '25-01'
        filename = f"{ticker}_{ts.strftime('%Y%m%dT%H%M%S')}.parquet"
        path = f"{S3_BUCKET}/ticker={ticker}/month={month_str}/{filename}"

        try:
            df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
            print(f"Saved: {path}")
        except Exception as e:
            print(f"Error saving file: {e}")

except KeyboardInterrupt:
    print("Interrupted by keyboard")
finally:
    consumer.close()