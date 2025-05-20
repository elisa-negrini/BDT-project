import json
import time
import pandas as pd
import s3fs
from kafka import KafkaConsumer

KAFKA_TOPIC = 'stock_trades'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'

S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'admin'
S3_SECRET_KEY = 'admin123'
S3_BUCKET = 'stock-data'

# === Connessione a Kafka ===
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
            print("✅ Connessione a Kafka riuscita (consumer).")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
            time.sleep(5)

consumer = connect_kafka_consumer()
print(f"📡 In ascolto sul topic '{KAFKA_TOPIC}'...")


# S3 filesystem (MinIO)
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT}
)

if not fs.exists(S3_BUCKET):
    fs.mkdir(S3_BUCKET)
    print(f"🪣 Bucket '{S3_BUCKET}' creato su MinIO.")
else:
    print(f"✅ Bucket '{S3_BUCKET}' già esistente.")

for message in consumer:
    data = message.value

    # Salta messaggi che non contengono i campi attesi
    required_keys = {"ticker", "timestamp", "price", "size", "exchange"}
    if not required_keys.issubset(data):
        print(f"⚠️ Messaggio scartato: {data}")
        continue

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
    print(f"✓ Salvato: {path}")



