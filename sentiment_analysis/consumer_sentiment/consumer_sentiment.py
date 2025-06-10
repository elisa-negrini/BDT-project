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
TOPICS = ['reddit_sentiment', 'news_sentiment', 'bluesky_sentiment']
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'sentiment-data'

# === Initialize S3 connection ===
def ensure_bucket_exists():
    """Ensures the configured S3 bucket exists, creating it if it doesn't."""
    s3 = boto3.resource(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    try:
        s3.meta.client.head_bucket(Bucket=S3_BUCKET)
    except ClientError:
        print(f"Bucket '{S3_BUCKET}' not found, creating it...")
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' successfully created.")

# === S3 file system (for pyarrow / s3fs) ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
)

# === Configure Kafka consumer ===
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'sentiment_consumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(TOPICS)

# === Startup ===
ensure_bucket_exists()
print(f"Listening to Kafka topics: {', '.join(TOPICS)}")

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
            print(f"JSON parsing error: {e}")
            continue

        timestamp = data.get("timestamp")
        tickers = data.get("ticker")
        social = data.get("social")
        score = data.get("sentiment_score")

        if not timestamp or not tickers or not social:
            print("Incomplete message, skipping. Raw message:")
            print(json.dumps(data, indent=2))
            continue

        try:
            ts = pd.to_datetime(timestamp, utc=True)
        except Exception as e:
            print(f"Timestamp parsing error: {e}, skipping.")
            continue

        for ticker in tickers:
            row = {
                "Social": social,
                "Ticker": ticker,
                "Timestamp": ts.isoformat(),
                "SentimentScore": score
            }
            df = pd.DataFrame([row])

            month_str = f"{ts.year % 100:02d}-{ts.month:02d}"  # e.g., '25-01'
            filename = f"{social}_{ticker}_{ts.strftime('%Y%m%dT%H%M%S')}.parquet"
            path = f"{S3_BUCKET}/social={social}/ticker={ticker}/month={month_str}/{filename}"

            try:
                df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
                print(f"Saved to: {path}")
            except Exception as e:
                print(f"Error saving Parquet file: {e}")

except KeyboardInterrupt:
    print("Interrupted by user")
finally:
    consumer.close()