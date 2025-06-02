import os
import json
import time
import pandas as pd
import s3fs
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# === Parametri ===
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = 'finnhub'

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'finnhub-data'

# === Connessione Kafka ===
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
            print("✅ Connessione a Kafka riuscita (consumer).")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
            time.sleep(5)

# === Controlla o crea bucket ===
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
        print(f"⚠️ Bucket '{S3_BUCKET}' non trovato, lo creo...")
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"✅ Bucket '{S3_BUCKET}' creato.")

# === Filesystem S3 ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
)

# === Avvio consumer ===
ensure_bucket_exists()
consumer = connect_kafka_consumer()
print(f"📡 In ascolto sul topic '{TOPIC_NAME}'...")

for message in consumer:
    data = message.value
    print(f"📨 Messaggio ricevuto: {data}")


    # Dati richiesti
    symbol = data.get("symbol_requested")
    timestamp_str = data.get("date")
    headline = data.get("headline")
    summary = data.get("summary", "")
    url = data.get("url", "")
    source = data.get("source", "")

    if not symbol or not timestamp_str:
        print("⚠️ Messaggio mancante di 'symbol_requested' o 'date', saltato.")
        continue

    try:
        timestamp = pd.to_datetime(timestamp_str, utc=True)
        date_str = timestamp.strftime("%Y-%m-%d")
        iso_timestamp = timestamp.isoformat()
    except Exception as e:
        print(f"⚠️ Errore parsing timestamp: {e}, saltato.")
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
        print(f"✓ Salvato: {s3_path}")
    except Exception as e:
        print(f"❌ Errore salvataggio Parquet: {e}")
