#!/usr/bin/env python3
"""
Consumer: Reads FRED-style Kafka records and writes to MinIO (partitioned by series and year).
"""

import os
import sys
import json
import signal
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, year
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import boto3

# === CONFIG ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "h_macrodata")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fred-minio-group")

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
MINIO_BUCKET = os.getenv("S3_BUCKET", "macro-data")

BATCH_SIZE = 10000  # Records per write

# === Logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
shutdown_flag = False

# === Signal Handling ===
def signal_handler(signum, frame):
    global shutdown_flag
    logger.info("🚑 Received shutdown signal")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# === Ensure Bucket Exists ===
def ensure_bucket_exists():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    try:
        buckets = s3.list_buckets()
        if not any(b['Name'] == MINIO_BUCKET for b in buckets.get('Buckets', [])):
            s3.create_bucket(Bucket=MINIO_BUCKET)
            logger.info(f"🫳 Bucket '{MINIO_BUCKET}' created")
        else:
            logger.info(f"✅ Bucket '{MINIO_BUCKET}' already exists")
    except Exception as e:
        logger.error(f"❌ Error ensuring bucket: {e}")
        raise

# === Create Spark Session ===
def create_spark():
    spark = SparkSession.builder \
        .appName("FredKafkaToMinIO") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

# === Kafka Consumer with Retry ===
def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=10000
            )
            logger.info("✅ Connessione a Kafka riuscita.")
            return consumer
        except NoBrokersAvailable as e:
            logger.warning(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
            time.sleep(5)

# === Save to MinIO ===
def save_to_minio(spark, records):
    if not records:
        return

    schema = StructType([
        StructField("series", StringType(), True),
        StructField("date", StringType(), True),
        StructField("value", DoubleType(), True)
    ])

    for rec in records:
        record_date = datetime.fromisoformat(rec["date"]).date()
        if record_date.year < 2021:
            continue

        rec["date"] = record_date
        df = spark.createDataFrame([rec], schema=schema) \
                  .withColumn("date", to_date(col("date"))) \
                  .withColumn("year", year(col("date")))

        df.write \
          .partitionBy("series", "year") \
          .mode("append") \
          .parquet(f"s3a://{MINIO_BUCKET}")

    logger.info(f"✅ Wrote {len(records)} records to MinIO")

# === Message Processor ===
def process_message(batch, message):
    try:
        if "series" in message and "date" in message and "value" in message:
            batch.append(message)
            return True
        else:
            logger.warning("⚠️ Skipping incomplete message: %s", message)
            return False
    except Exception as e:
        logger.error(f"❌ Error parsing message: {e}")
        return False

# === Main Loop ===
def main():
    global shutdown_flag
    logger.info("🚀 Starting Kafka → MinIO consumer")

    try:
        ensure_bucket_exists()
        spark = create_spark()
        consumer = connect_kafka_consumer()
    except Exception as e:
        logger.error(f"❌ Initialization failed: {e}")
        sys.exit(1)

    batch = []
    processed = 0
    errors = 0

    try:
        while not shutdown_flag:
            polled = consumer.poll(timeout_ms=1000)
            if not polled:
                continue

            for tp, msgs in polled.items():
                for msg in msgs:
                    if shutdown_flag:
                        break
                    success = process_message(batch, msg.value)
                    if success:
                        processed += 1
                    else:
                        errors += 1

                    if len(batch) >= BATCH_SIZE:
                        save_to_minio(spark, batch)
                        batch.clear()

        # Final flush
        if batch:
            save_to_minio(spark, batch)

    finally:
        logger.info(f"📊 Processed: {processed}, Errors: {errors}")
        consumer.close()
        spark.stop()
        logger.info("🖚 Shutdown complete")

if __name__ == "__main__":
    main()
