import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd
import s3fs
import os

# === Configurazione Kafka ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "h_macrodata"
KAFKA_GROUP_ID = "macro-consumer-group"

# === Configurazione MinIO ===
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET = "macro-data"

# === Connessione a MinIO via s3fs ===
fs = s3fs.S3FileSystem(
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    client_kwargs={'endpoint_url': f"http://{MINIO_ENDPOINT}"}
)

# === Kafka consumer con retry ===
def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            print("✅ Connessione a Kafka riuscita (consumer).")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
            time.sleep(5)

# === Salvataggio su MinIO ===
def save_record_to_minio(record):
    try:
        alias = record["series"]  # Questo è l'alias, come "cpi", "unemployment", ecc.
        date_str = record["date"]
        value = record["value"]

        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        year = date_obj.year
        filename = f"{alias}_{date_str}.parquet"
        s3_path = f"{MINIO_BUCKET}/{alias}/{year}/{filename}"

        df = pd.DataFrame([{
            "series": alias,
            "date": date_str,
            "value": value
        }])

        with fs.open(s3_path, 'wb') as f:
            df.to_parquet(f, index=False)

        print(f"💾 Salvato su MinIO: {s3_path}")
    except Exception as e:
        print(f"❌ Errore salvataggio su MinIO: {e}")

# === Main loop ===
def main():
    consumer = connect_kafka_consumer()
    print("👂 In ascolto su Kafka...")

    try:
        for message in consumer:
            record = message.value
            save_record_to_minio(record)
    except KeyboardInterrupt:
        print("🛑 Interrotto da utente.")
    finally:
        consumer.close()
        print("🔚 Consumer Kafka chiuso.")

if __name__ == "__main__":
    main()
