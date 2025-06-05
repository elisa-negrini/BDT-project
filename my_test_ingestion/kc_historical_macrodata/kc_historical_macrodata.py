import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd
import s3fs
import sys # Import sys for critical exits

# === Kafka Configuration ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = "h_macrodata"
KAFKA_GROUP_ID = "macro-consumer-group"

# === MinIO Configuration ===
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET = "macro-data"

# === MinIO Connection via s3fs ===
# Validate MinIO configuration before attempting connection
if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    print("Error: One or more MinIO environment variables (S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY) are not set. Exiting.")
    sys.exit(1)

try:
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={'endpoint_url': f"http://{MINIO_ENDPOINT}"}
    )
    # Optional: Test connection to ensure credentials are valid
    # fs.ls('/') # This would list root directories, might throw error if credentials are bad
    print("Successfully connected to MinIO filesystem via s3fs.")
except Exception as e:
    print(f"Error connecting to MinIO via s3fs: {e}. Please check your credentials and endpoint. Exiting.")
    sys.exit(1)


# === Kafka Consumer with Retry ===
def connect_kafka_consumer():
    """
    Establishes and returns a KafkaConsumer connection.
    Retries indefinitely if Kafka brokers are not available.
    """
    if not KAFKA_BOOTSTRAP_SERVERS:
        print("Error: KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
        sys.exit(1)

    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest', # Start reading from the earliest available offset if no committed offset
                enable_auto_commit=True       # Automatically commit offsets
            )
            print("Successfully connected to Kafka (consumer).")
            return consumer
        except Exception as e:
            print(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
            time.sleep(5)

# === Save Record to MinIO ===
def save_record_to_minio(record):
    """
    Saves a single macroeconomic data record as a Parquet file to MinIO,
    partitioned by series alias and year.
    """
    try:
        alias = record["series"]
        date_str = record["date"]
        value = record["value"]

        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        year = date_obj.year
        filename = f"{alias}_{date_str}.parquet"
        
        # Path in MinIO: bucket/series_alias/year/filename.parquet
        s3_path = f"{MINIO_BUCKET}/{alias}/{year}/{filename}"

        # Create a DataFrame for the single record
        df = pd.DataFrame([{
            "series": alias,
            "date": date_str,
            "value": value
        }])

        # Write DataFrame to Parquet directly to MinIO using s3fs
        with fs.open(s3_path, 'wb') as f:
            df.to_parquet(f, index=False)

        print(f"Saved to MinIO: {s3_path}")
    except Exception as e:
        print(f"Error saving record to MinIO: {e}")

# === Main Loop ===
def main():
    """
    Main function to consume messages from Kafka and save them to MinIO.
    """
    consumer = connect_kafka_consumer()
    print(f"Listening for messages on Kafka topic: {KAFKA_TOPIC}...")

    try:
        # Iterate over messages from Kafka
        for message in consumer:
            record = message.value
            save_record_to_minio(record)
    except KeyboardInterrupt:
        # Handle user interruption (Ctrl+C)
        print("Interrupted by user.")
    finally:
        # Ensure the Kafka consumer is closed when done or interrupted
        consumer.close()
        print("Kafka consumer closed.")

if __name__ == "__main__":
    main()