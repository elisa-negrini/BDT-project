import os
import json
import time
import pandas as pd
import s3fs
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# === Configurable parameters ===
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = 'bluesky'

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'bluesky-data'

# === Connect to Kafka ===
def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='json_saver_group'
            )
            print("Kafka consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

# === Create bucket if it does not exist ===
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
        print(f"Bucket '{S3_BUCKET}' not found, creating it...")
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' created successfully.")

# === S3 filesystem ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
)

# === Start consumer ===
ensure_bucket_exists()
consumer = connect_kafka_consumer()
print(f"Listening on topic '{TOPIC_NAME}'...")

for message in consumer:
    data = message.value

    created_at = data.get("created_at")
    keyword = data.get("keyword")
    text = data.get("text")

    if not created_at or not keyword:
        print("Message missing 'created_at' or 'keyword', skipping.")
        continue

    try:
        timestamp = pd.to_datetime(created_at, utc=True).isoformat()
    except Exception as e:
        print(f"Error parsing timestamp: {e}, skipping.")
        continue

    row = {
        "Keyword": keyword,
        "Text": text or "",
        "Timestamp": timestamp
    }

    df = pd.DataFrame([row])
    date = timestamp[:10]

    filename = f"bluesky_{timestamp}.parquet"
    path = f"{S3_BUCKET}/date={date}/keyword={keyword}/{filename}"

    df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
    print(f"Saved file: {path}")