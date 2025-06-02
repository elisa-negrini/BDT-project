#!/usr/bin/env python3

import os
import json
import logging
import signal
import sys
import time
from datetime import datetime
from kafka import KafkaConsumer
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO

# === CONFIG ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "h_alpaca")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumer_alpaca")

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
MINIO_BUCKET = os.getenv("S3_BUCKET", "historical-data")

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("simple-consumer")

# === SEGNALE DI STOP ===
shutdown_flag = False
def signal_handler(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    logger.info("🛑 Shutdown richiesto...")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# === S3 CLIENT ===
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

def connect_kafka_consumer():
    """Tenta la connessione a Kafka consumer con retry fino a successo"""
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=KAFKA_GROUP_ID,
                consumer_timeout_ms=10000
            )
            logger.info("✅ Connessione a Kafka riuscita (consumer).")
            return consumer
        except Exception as e:
            logger.warning(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
            time.sleep(5)

def save_to_minio(ticker, year, month, date_str, records):
    if not records:
        return False

    try:
        for r in records:
            r['ticker'] = ticker

        df = pd.DataFrame(records)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        table = pa.Table.from_pandas(df)
        buf = BytesIO()
        pq.write_table(table, buf, compression='snappy')
        buf.seek(0)

        key = f"{ticker}/{year}/{month:02d}/{ticker}_{date_str}.parquet"
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.getvalue())
        logger.info(f"✅ Salvato su MinIO: {key} ({len(df)} record)")
        return True

    except Exception as e:
        logger.error(f"❌ Errore salvataggio Parquet per {ticker} {date_str}: {e}")
        return False

def main():
    logger.info("🚀 Avvio consumer semplificato Kafka -> MinIO")

    consumer = connect_kafka_consumer()
    processed = 0
    errors = 0

    max_inactivity_seconds = 60  # 🔚 Dopo 60 secondi senza messaggi, chiudo
    last_message_time = time.time()

    try:
        while not shutdown_flag:
            try:
                message_pack = consumer.poll(timeout_ms=1000)
                if message_pack:
                    last_message_time = time.time()  # 🔄 resetta il timer all'arrivo di messaggi

                for tp, messages in message_pack.items():
                    for message in messages:
                        if shutdown_flag:
                            break

                        try:
                            payload = message.value
                            ticker = payload['ticker']
                            year = payload['year']
                            month = payload['month']
                            date_str = payload['date']
                            records = payload['data']

                            if save_to_minio(ticker, year, month, date_str, records):
                                processed += 1
                            else:
                                errors += 1

                        except Exception as e:
                            logger.error(f"❌ Errore elaborazione messaggio: {e}")
                            errors += 1
                            
                # 🔚 Se è passato troppo tempo senza nuovi messaggi
                if time.time() - last_message_time > max_inactivity_seconds:
                    logger.info(f"⏳ Nessun messaggio da {max_inactivity_seconds} secondi. Esco.")
                    break

            except Exception as e:
                logger.error(f"❌ Errore nel ciclo di polling: {e}")
                break

    finally:
        consumer.close()
        logger.info(f"🔚 Consumer terminato. Processati: {processed}, Errori: {errors}")

if __name__ == "__main__":
    main()
