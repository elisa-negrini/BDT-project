import os
import json
import time
import pandas as pd
import s3fs
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# === Parametri configurabili ===
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'bluesky'

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'admin')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'admin123')
S3_BUCKET = os.getenv('S3_BUCKET', 'bluesky-data')

# === Connessione a Kafka ===
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
            print("✅ Connessione a Kafka riuscita (consumer).")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
            time.sleep(5)

# === Crea bucket se non esiste ===
def ensure_bucket_exists():
    s3 = boto3.resource(
        's3',
        endpoint_url=S3_ENDPOINT,
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
    client_kwargs={'endpoint_url': S3_ENDPOINT}
)

# === Avvia Consumer ===
ensure_bucket_exists()
consumer = connect_kafka_consumer()
print(f"📡 In ascolto sul topic '{TOPIC_NAME}'...")

for message in consumer:
    data = message.value

    created_at = data.get("created_at")
    keyword = data.get("keyword")
    text = data.get("text")

    if not created_at or not keyword:
        print("⚠️ Messaggio mancante di 'created_at' o 'keyword', saltato.")
        continue

    try:
        timestamp = pd.to_datetime(created_at, utc=True).isoformat()
    except Exception as e:
        print(f"⚠️ Errore parsing timestamp: {e}, saltato.")
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
    print(f"✓ Salvato: {path}")



