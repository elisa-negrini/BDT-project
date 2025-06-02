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


# === Parametri configurabili ===
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = 'macrodata'

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = 'macro-data'

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
                group_id='macrodata_saver_group'
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

# === Avvia Consumer ===
ensure_bucket_exists()
consumer = connect_kafka_consumer()
print(f"📡 In ascolto sul topic '{TOPIC_NAME}'...")

for message in consumer:
    data = message.value

    alias = data.get("alias")
    date = data.get("date")
    value = data.get("value")

    if not alias or not date or value is None:
        print("⚠️ Messaggio incompleto, saltato:", data)
        continue

    try:
        dt = pd.to_datetime(date)
        date_str = dt.strftime("%Y-%m-%d")
        year = dt.year
    except Exception as e:
        print(f"⚠️ Errore parsing data: {e}, saltato.")
        continue

    row = {
        "Date": date_str,
        "Value": value
    }

    df = pd.DataFrame([row])
    
    # Path compatibile con lo storico
    filename = f"{alias}_{date_str}.parquet"
    path = f"{S3_BUCKET}/{alias}/{year}/{filename}"

    try:
        df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
        print(f"✓ Salvato: {path}")
    except Exception as e:
        print(f"❌ Errore nel salvataggio del file {filename}: {e}")

import pandas as pd
from pyarrow.fs import S3FileSystem
from datetime import datetime

# === Connessioni iniziali ===
fs = S3FileSystem(endpoint_override="http://minio:9000", access_key="minioadmin", secret_key="minioadmin")  # Adatta se diverso
ensure_bucket_exists()
consumer = connect_kafka_consumer()


