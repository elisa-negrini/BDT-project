import json
import time
import pandas as pd
import s3fs
from kafka import KafkaConsumer

KAFKA_TOPIC = 'stock_alpaca'
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

for message in consumer:
    data = message.value

    row = {
        "Ticker": data["symbol"],
        "Timestamp": pd.to_datetime(data["timestamp"], utc=True).isoformat(),
        "Price": data["price"],
        "Size": data["size"],
        "Exchange": data["exchange"]
    }

    df = pd.DataFrame([row])
    date = df["Timestamp"].iloc[0][:10]
    ticker = df["Ticker"].iloc[0]

    filename = f"stock_{df['Timestamp'].iloc[0]}.parquet"
    path = f"{S3_BUCKET}/stock/ticker={ticker}/date={date}/{filename}"

    df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
    print(f"✓ Salvato: {path}")

