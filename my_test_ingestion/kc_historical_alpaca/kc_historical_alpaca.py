#!/usr/bin/env python3
"""
Consumer: Riceve dati da Kafka e li salva su MinIO tramite Spark (in batch ottimizzati)
"""

import json
import os
import sys
import signal
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import boto3
import logging

# === CONFIGURAZIONE ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "h_alpaca")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "minio-consumer-group")

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
MINIO_BUCKET = os.getenv("S3_BUCKET", "historical-data")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

shutdown_flag = False

def signal_handler(signum, frame):
    global shutdown_flag
    logger.info("🛑 Ricevuto segnale di shutdown")
    shutdown_flag = True

def ensure_bucket_exists(bucket_name, endpoint, access_key, secret_key):
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    try:
        buckets = s3.list_buckets()
        if not any(b['Name'] == bucket_name for b in buckets.get('Buckets', [])):
            s3.create_bucket(Bucket=bucket_name)
            logger.info(f"🪣 Bucket '{bucket_name}' creato.")
        else:
            logger.info(f"✅ Bucket '{bucket_name}' già esistente.")
    except Exception as e:
        logger.error(f"❌ Errore controllo/creazione bucket: {e}")
        raise

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("KafkaToMinIO") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Sessione Spark creata e configurata")
        return spark

    except Exception as e:
        logger.error(f"❌ Errore creazione sessione Spark: {e}")
        raise

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

def save_to_minio(spark, data):
    try:
        if not data:
            return False

        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", FloatType(), True),
            StructField("trade_count", FloatType(), True),
            StructField("vwap", FloatType(), True),
        ])

        for record in data:
            if isinstance(record.get('timestamp'), str):
                record['timestamp'] = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))

        df = spark.createDataFrame(data, schema=schema)
        df = df.withColumn("date", to_date("timestamp"))
        df = df.withColumn("year", col("date").substr(1, 4))
        df = df.withColumn("month", col("date").substr(6, 2))

        df.coalesce(10).write \
            .partitionBy("symbol", "year", "month") \
            .option("compression", "snappy") \
            .mode("append") \
            .parquet(f"s3a://{MINIO_BUCKET}")

        logger.info(f"✅ Scrittura completata: {len(data)} record")
        return True

    except Exception as e:
        logger.error(f"❌ Errore salvataggio batch: {e}")
        return False

def process_message(batch, message):
    try:
        data = message['data']
        for record in data:
            record['symbol'] = message['ticker']
        batch.extend(data)
        return True
    except Exception as e:
        logger.error(f"❌ Errore elaborazione messaggio: {e}")
        return False

def main():
    global shutdown_flag
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("🚀 Avvio Consumer Kafka -> MinIO")

    try:
        ensure_bucket_exists(MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
        spark = create_spark_session()
        consumer = connect_kafka_consumer()
    except Exception as e:
        logger.error(f"❌ Errore inizializzazione: {e}")
        sys.exit(1)

    processed_count = 0
    error_count = 0
    batch_data = []
    BATCH_SIZE = 100000

    try:
        logger.info(f"👂 In ascolto su topic '{KAFKA_TOPIC}'...")

        while not shutdown_flag:
            try:
                message_pack = consumer.poll(timeout_ms=1000)
                if not message_pack:
                    continue

                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        if shutdown_flag:
                            break

                        try:
                            success = process_message(batch_data, message.value)
                            if success:
                                processed_count += 1
                            else:
                                error_count += 1

                            if len(batch_data) >= BATCH_SIZE:
                                save_to_minio(spark, batch_data)
                                batch_data.clear()

                        except Exception as e:
                            logger.error(f"❌ Errore elaborazione messaggio: {e}")
                            error_count += 1

            except KafkaError as e:
                logger.error(f"❌ Errore Kafka: {e}")
            except Exception as e:
                logger.error(f"❌ Errore generico: {e}")

        if batch_data:
            save_to_minio(spark, batch_data)
            batch_data.clear()

    except KeyboardInterrupt:
        logger.info("⏹️  Interruzione da utente")
    finally:
        logger.info(f"📊 Statistiche: Processati: {processed_count}, Errori: {error_count}")

        try:
            consumer.close()
            logger.info("🔚 Consumer Kafka chiuso")
        except:
            pass

        try:
            spark.stop()
            logger.info("🔚 Sessione Spark chiusa")
        except:
            pass

if __name__ == "__main__":
    main()
