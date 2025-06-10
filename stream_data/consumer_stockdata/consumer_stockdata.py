import json
import time
import pandas as pd
import s3fs
import pytz
from kafka import KafkaConsumer
import os

# === Configuration ===
KAFKA_TOPIC = 'stock_trades'
KAFKA_BROKER = "kafka:9092"

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'stock-data'

# New York timezone for market hour validation.
ny_timezone = pytz.timezone("America/New_York")

# === Kafka Connection ===
def connect_kafka_consumer():
    """
    Establishes and returns a Kafka consumer, with retry logic.
    """
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest', # Start consuming from the beginning if no offset is committed.
                enable_auto_commit=True,
                group_id="reddit_saver" # Consumer group ID.
            )
            print("Successfully connected to Kafka (consumer).")
            return consumer
        except Exception as e:
            print(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
            time.sleep(5)

consumer = connect_kafka_consumer()
print(f"Listening on topic '{KAFKA_TOPIC}'...")

# === S3 (MinIO) Filesystem Setup ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
)

if not fs.exists(S3_BUCKET):
    fs.mkdir(S3_BUCKET)
    print(f"Bucket '{S3_BUCKET}' created on MinIO.")
else:
    print(f"Bucket '{S3_BUCKET}' already exists.")

# === Market Hours Validation ===
def is_within_market_hours(timestamp_str):
    """
    Checks if a given UTC timestamp falls within New York stock market hours
    (9:30 AM to 4:00 PM ET, Monday to Friday).
    """
    dt_utc = pd.to_datetime(timestamp_str)
    dt_ny = dt_utc.tz_convert(ny_timezone)

    if dt_ny.weekday() >= 5:  # Check if it's a weekday (0=Monday, 6=Sunday)
        return False

    market_open_time = dt_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = dt_ny.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open_time <= dt_ny < market_close_time

# === Main Consumption Loop ===
for message in consumer:
    data = message.value

    required_keys = {"ticker", "timestamp", "price", "size", "exchange"}
    if not required_keys.issubset(data):
        print(f"Message discarded: Missing required keys: {data}")
        continue

    if not is_within_market_hours(data["timestamp"]):
        continue

    row = {
        "ticker": data["ticker"],
        "timestamp": data["timestamp"],
        "price": float(data["price"]),.
        "size": data["size"],
        "exchange": data["exchange"]
    }
    df = pd.DataFrame([row])

    date = df["timestamp"].iloc[0][:10]
    ticker = df["ticker"].iloc[0]

    filename = f"stock_{df['timestamp'].iloc[0].replace(':', '-')}.parquet"
    path = f"{S3_BUCKET}/ticker={ticker}/date={date}/{filename}"

    df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
    print(f"Saved: {path}")