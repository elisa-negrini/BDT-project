import json
import time
import s3fs
from kafka import KafkaConsumer
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import os

# === Configuration ===
KAFKA_TOPIC = 'reddit'
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'reddit-data'

# === Connect to Kafka ===
def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id="reddit_saver"
            )
            print("Kafka consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

# === Connect to MinIO via S3FS ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
)

# === Create bucket if it does not exist ===
if not fs.exists(S3_BUCKET):
    fs.mkdir(S3_BUCKET)
    print(f"Bucket '{S3_BUCKET}' created on MinIO.")
else:
    print(f"Bucket '{S3_BUCKET}' already exists.")

# === Start consuming messages ===
consumer = connect_kafka_consumer()
print(f"Listening to topic '{KAFKA_TOPIC}'...")

for message in consumer:
    data = message.value

    # Build the record dictionary (adapt field names as needed)
    record = {
        "id": [data.get("id")],
        "author": [data.get("author")],
        "title": [data.get("title")],
        "text": [data.get("text")],
        "subreddit": [data.get("subreddit")],
        "created_utc": [datetime.utcfromtimestamp(data.get("created_utc")).isoformat()]
    }

    table = pa.Table.from_pydict(record)

    # Create path with subreddit and date
    subreddit = record["subreddit"][0] or "unknown"
    date = record["created_utc"][0][:10]
    filename = f"reddit_{record['id'][0]}.parquet"
    path = f"{S3_BUCKET}/date={date}/subreddit={subreddit}/{filename}"

    # Write to MinIO
    with fs.open(f"s3://{path}", 'wb') as f:
        pq.write_table(table, f)

    print(f"File saved: {path}")