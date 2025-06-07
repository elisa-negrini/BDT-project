import os
import json
import time
import pandas as pd
import s3fs
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import ClientError
from pyarrow.fs import S3FileSystem
from datetime import datetime

# === Configurable parameters ===
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = 'macrodata'

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'macro-data'

# === Kafka connection ===
def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='macrodata_saver_group'
            )
            print("Kafka consumer connection established successfully.")
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
        print(f"Bucket '{S3_BUCKET}' not found. Creating it...")
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' created successfully.")

# === S3 filesystem ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
)

# === Start Kafka consumer ===
ensure_bucket_exists()
consumer = connect_kafka_consumer()
print(f"Listening to topic '{TOPIC_NAME}'...")

for message in consumer:
    data = message.value

    alias = data.get("alias")
    date = data.get("date")
    value = data.get("value")

    if not alias or not date or value is None:
        print("Incomplete message received, skipping:", data)
        continue

    try:
        dt = pd.to_datetime(date)
        date_str = dt.strftime("%Y-%m-%d")
        year = dt.year
    except Exception as e:
        print(f"Error parsing date: {e}, skipping.")
        continue

    row = {
        "Date": date_str,
        "Value": value
    }

    df = pd.DataFrame([row])
    
    # Path structure compatible with historical data
    filename = f"{alias}_{date_str}.parquet"
    path = f"{S3_BUCKET}/{alias}/{year}/{filename}"

    try:
        df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
        print(f"File saved: {path}")
    except Exception as e:
        print(f"Error saving file {filename}: {e}")

# da togliere ? 

# # Optional init section (if used elsewhere)
# fs = S3FileSystem(
#     endpoint_override="http://minio:9000",
#     access_key="minioadmin",
#     secret_key="minioadmin"
# )
# ensure_bucket_exists()
# consumer = connect_kafka_consumer()