# import json
# import time
# import pandas as pd
# import s3fs
# from kafka import KafkaConsumer

# KAFKA_TOPIC = 'stock_trades'
# KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'

# S3_ENDPOINT = 'http://minio:9000'
# S3_ACCESS_KEY = 'admin'
# S3_SECRET_KEY = 'admin123'
# S3_BUCKET = 'stock-data'

# # === Connessione a Kafka ===
# def connect_kafka_consumer():
#     while True:
#         try:
#             consumer = KafkaConsumer(
#                 KAFKA_TOPIC,
#                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#                 auto_offset_reset='earliest',
#                 enable_auto_commit=True,
#                 group_id="reddit_saver"
#             )
#             print("✅ Connessione a Kafka riuscita (consumer).")
#             return consumer
#         except Exception as e:
#             print(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
#             time.sleep(5)

# consumer = connect_kafka_consumer()
# print(f"📡 In ascolto sul topic '{KAFKA_TOPIC}'...")


# # S3 filesystem (MinIO)
# fs = s3fs.S3FileSystem(
#     anon=False,
#     key=S3_ACCESS_KEY,
#     secret=S3_SECRET_KEY,
#     client_kwargs={'endpoint_url': S3_ENDPOINT}
# )

# if not fs.exists(S3_BUCKET):
#     fs.mkdir(S3_BUCKET)
#     print(f"🪣 Bucket '{S3_BUCKET}' creato su MinIO.")
# else:
#     print(f"✅ Bucket '{S3_BUCKET}' già esistente.")

# for message in consumer:
#     data = message.value

#     # Salta messaggi che non contengono i campi attesi
#     required_keys = {"ticker", "timestamp", "price", "size", "exchange"}
#     if not required_keys.issubset(data):
#         print(f"⚠️ Messaggio scartato: {data}")
#         continue

#     row = {
#         "ticker": data["ticker"],
#         "timestamp": data["timestamp"],
#         "price": float(data["price"]),
#         "size": data["size"],
#         "exchange": data["exchange"]
#     }

#     df = pd.DataFrame([row])
#     date = df["timestamp"].iloc[0][:10]
#     ticker = df["ticker"].iloc[0]

#     filename = f"stock_{df['timestamp'].iloc[0]}.parquet"
#     path = f"{S3_BUCKET}/ticker={ticker}/date={date}/{filename}"

#     df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
#     print(f"✓ Salvato: {path}")









import json
import time
import pandas as pd
import s3fs
import pytz
from kafka import KafkaConsumer
import os

KAFKA_TOPIC = 'stock_trades'
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'stock-data'

# New York timezone for market hours check
ny_timezone = pytz.timezone("America/New_York")

# === Kafka Connection ===
def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id="reddit_saver"
            )
            print("Successfully connected to Kafka (consumer).")
            return consumer
        except Exception as e:
            print(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
            time.sleep(5)

consumer = connect_kafka_consumer()
print(f"Listening on topic '{KAFKA_TOPIC}'...")

# S3 filesystem (MinIO)
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

# Function to check if the timestamp falls within market hours (9:30 - 16:00 ET)
def is_within_market_hours(timestamp_str):
    # Convert timestamp string to datetime object.
    # It's already timezone-aware (UTC) from the producer, so no need for tz_localize.
    dt_utc = pd.to_datetime(timestamp_str)
    
    # Convert to New York time
    dt_ny = dt_utc.tz_convert(ny_timezone)

    # Check if it's a weekday (0=Monday, 6=Sunday)
    if dt_ny.weekday() >= 5: # Saturday or Sunday
        return False

    # Define market open and close times for the current day in NY timezone
    market_open_time = dt_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = dt_ny.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open_time <= dt_ny < market_close_time

for message in consumer:
    data = message.value

    # Skip messages that do not contain the expected fields
    required_keys = {"ticker", "timestamp", "price", "size", "exchange"}
    if not required_keys.issubset(data):
        print(f"Message discarded: Missing required keys: {data}")
        continue

    # --- Filtering Logic ---
    if not is_within_market_hours(data["timestamp"]):
        print(f"Message discarded: Outside market hours: {data}") # Uncomment for debugging outside market hours
        continue
    # --- End Filtering Logic ---

    row = {
        "ticker": data["ticker"],
        "timestamp": data["timestamp"],
        "price": float(data["price"]),
        "size": data["size"],
        "exchange": data["exchange"]
    }

    df = pd.DataFrame([row])
    date = df["timestamp"].iloc[0][:10]
    ticker = df["ticker"].iloc[0]

    filename = f"stock_{df['timestamp'].iloc[0]}.parquet"
    path = f"{S3_BUCKET}/ticker={ticker}/date={date}/{filename}"

    df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
    print(f"Saved: {path}")