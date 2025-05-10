import json
import time
import pandas as pd
import s3fs
from kafka import KafkaConsumer
from datetime import datetime

# === Configurazioni ===
KAFKA_TOPIC = 'reddit'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'

S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'admin'
S3_SECRET_KEY = 'admin123'
S3_BUCKET = 'reddit-data'

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

# === Connessione a MinIO ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT}
)

# === Crea il bucket solo se non esiste ===
if not fs.exists(S3_BUCKET):
    fs.mkdir(S3_BUCKET)
    print(f"🪣 Bucket '{S3_BUCKET}' creato su MinIO.")
else:
    print(f"✅ Bucket '{S3_BUCKET}' già esistente.")

# === Inizio consumo ===
consumer = connect_kafka_consumer()
print(f"📡 In ascolto sul topic '{KAFKA_TOPIC}'...")

for message in consumer:
    data = message.value

    # Costruisci il DataFrame (adatta alle tue chiavi reali)
    df = pd.DataFrame([{
        "id": data.get("id"),
        "author": data.get("author"),
        "title": data.get("title"),
        "body": data.get("body"),
        "subreddit": data.get("subreddit"),
        "created_utc": pd.to_datetime(data.get("created_utc"), unit='s', utc=True).isoformat()
    }])

    # Crea percorso con subreddit e data
    subreddit = df["subreddit"].iloc[0] or "unknown"
    date = df["created_utc"].iloc[0][:10]
    filename = f"reddit_{df['id'].iloc[0]}.parquet"
    path = f"{S3_BUCKET}/subreddit={subreddit}/date={date}/{filename}"

    # Salva su MinIO
    df.to_parquet(f"s3://{path}", engine='pyarrow', filesystem=fs, index=False)
    print(f"💾 Salvato: {path}")
