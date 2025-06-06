#!/usr/bin/env python3
import os
import json
import time
import pandas as pd
import boto3
from io import BytesIO
from kafka import KafkaConsumer
import logging
import sys # Import sys for sys.exit

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaToMinio")

# === CONFIGURATION ===
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "h_company"
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET ="company-fundamentals"

# === KAFKA CONSUMER FUNCTION ===
def connect_kafka_consumer():
    """
    Establishes and returns a KafkaConsumer connection.
    Retries indefinitely if Kafka brokers are not available.
    """
    if not KAFKA_BOOTSTRAP_SERVERS:
        logger.critical("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
        sys.exit(1)

    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest', # Start reading from the beginning of the topic if no committed offset is found
                enable_auto_commit=True,      # Automatically commit offsets
                consumer_timeout_ms=10000     # Timeout after 10 seconds if no message is received
            )
            logger.info("Successfully connected to Kafka (consumer).")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
            time.sleep(5)

def ensure_bucket_exists(bucket_name, endpoint, access_key, secret_key):
    """
    Ensures that the specified MinIO/S3 bucket exists. Creates it if it doesn't.
    """
    if not all([endpoint, access_key, secret_key]):
        logger.critical("MinIO/S3 environment variables (ENDPOINT_URL, ACCESS_KEY, SECRET_KEY) are not set. Exiting.")
        sys.exit(1)

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
            logger.info(f"Bucket '{bucket_name}' created.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        logger.error(f"Error checking/creating bucket: {e}")
        raise # Re-raise the exception to stop execution if bucket cannot be assured

# === START CONSUMER AND COLLECT RECORDS ===
consumer = connect_kafka_consumer()
logger.info("Listening for messages on Kafka...")

records = []
try:
    for message in consumer:
        records.append(message.value)
        # Log symbol if available in the message value, otherwise 'N/A'
        logger.info(f"Received record for symbol: {message.value.get('symbol', 'N/A')}")
except Exception as e:
    logger.error(f"Error consuming messages from Kafka: {e}")
finally:
    consumer.close() # Ensure consumer is closed even if an error occurs

if not records:
    logger.warning("No data received from Kafka within the timeout period. Exiting.")
    sys.exit(0) # Exit gracefully if no records were consumed

# === CONVERT TO PANDAS DATAFRAME ===
df = pd.DataFrame(records)

# Filter out rows where 'calendarYear' is missing, as it's critical for partitioning
df = df[df['calendarYear'].notna()]

# Check for essential columns required for processing
required_columns = {'symbol', 'calendarYear'}
if not required_columns.issubset(df.columns):
    logger.error(f"Missing required columns in DataFrame: {required_columns - set(df.columns)}. Exiting.")
    sys.exit(1)

# Ensure calendarYear is integer for consistent partitioning
df['calendarYear'] = df['calendarYear'].astype(int)

# === WRITE TO MINIO ===
ensure_bucket_exists(MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

logger.info(f"Starting to upload data to MinIO bucket '{MINIO_BUCKET}'...")
uploaded_files_count = 0
try:
    # Group DataFrame by 'symbol' and 'calendarYear' to create partitioned Parquet files
    for (symbol, year), group_df in df.groupby(['symbol', 'calendarYear']):
        buffer = BytesIO()
        group_df.to_parquet(buffer, index=False) # Write group to in-memory buffer
        buffer.seek(0) # Reset buffer position to the beginning
        
        key = f"{symbol}/{year}.parquet" # Define object key (path in bucket)
        s3.upload_fileobj(buffer, MINIO_BUCKET, key) # Upload the buffer content
        logger.info(f"Uploaded: {key}")
        uploaded_files_count += 1
except Exception as e:
    logger.error(f"An error occurred during file upload to MinIO: {e}")
    sys.exit(1)

logger.info(f"Successfully saved {uploaded_files_count} files to MinIO.")