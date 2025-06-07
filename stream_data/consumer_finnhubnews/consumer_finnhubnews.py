import os
import json
import time
import pandas as pd
import s3fs
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# === Parameters ===
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = 'finnhub'

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'finnhub-data'

# === Kafka Connection ===
def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='finnhub_saver_group_v2'
            )
            print("Kafka consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

# === Ensure bucket exists ===
def ensure_bucket_exists():
    s3 = boto3.resource(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    try:
        s3.meta.client.head_bucket(Bucket=S3_BUCKET)
    except ClientError:
        print(f"Bucket '{S3_BUCKET}' not found. Creating it...")
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' created successfully.")

# === S3 Filesystem ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
)

# === Start consumer ===
ensure_bucket_exists()
consumer = connect_kafka_consumer()
print(f"Listening to topic '{TOPIC_NAME}'...")

for message in consumer:
    data = message.value
    print(f"Message received: {data}")

    # Required fields
    symbol = data.get("symbol_requested")
    timestamp_str = data.get("date")
    headline = data.get("headline")
    summary = data.get("summary", "")
    url = data.get("url", "")
    source = data.get("source", "")

    if not symbol or not timestamp_str:
        print("Message missing 'symbol_requested' or 'date'. Skipping.")
        continue

    try:
        timestamp = pd.to_datetime(timestamp_str, utc=True)
        date_str = timestamp.strftime("%Y-%m-%d")
        iso_timestamp = timestamp.isoformat()
    except Exception as e:
        print(f"Error parsing timestamp: {e}. Skipping.")
        continue

    row = {
        "Symbol": symbol,
        "Timestamp": iso_timestamp,
        "Headline": headline,
        "Summary": summary,
        "URL": url,
        "Source": source
    }

    df = pd.DataFrame([row])
    filename = f"finnhub_{timestamp.strftime('%Y%m%dT%H%M%S')}.parquet"
    s3_path = f"{S3_BUCKET}/ticker={symbol}/date={date_str}/{filename}"

    try:
        df.to_parquet(f"s3://{s3_path}", engine="pyarrow", filesystem=fs, index=False)
        print(f"Saved file: {s3_path}")
    except Exception as e:
        print(f"Error saving Parquet file: {e}")
