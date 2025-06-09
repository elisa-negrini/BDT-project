# import os
# import json
# import time
# from datetime import datetime
# from kafka import KafkaConsumer
# import pandas as pd
# import s3fs
# import sys # Import sys for critical exits

# # === Kafka Configuration ===
# KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# KAFKA_TOPIC = "h_macrodata"
# KAFKA_GROUP_ID = "macro-consumer-group"

# # === MinIO Configuration ===
# MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
# MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
# MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
# MINIO_BUCKET = "macro-data"

# # === MinIO Connection via s3fs ===
# # Validate MinIO configuration before attempting connection
# if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
#     print("Error: One or more MinIO environment variables (S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY) are not set. Exiting.")
#     sys.exit(1)

# try:
#     fs = s3fs.S3FileSystem(
#         key=MINIO_ACCESS_KEY,
#         secret=MINIO_SECRET_KEY,
#         client_kwargs={'endpoint_url': f"http://{MINIO_ENDPOINT}"}
#     )
#     # Optional: Test connection to ensure credentials are valid
#     # fs.ls('/') # This would list root directories, might throw error if credentials are bad
#     print("Successfully connected to MinIO filesystem via s3fs.")
# except Exception as e:
#     print(f"Error connecting to MinIO via s3fs: {e}. Please check your credentials and endpoint. Exiting.")
#     sys.exit(1)


# # === Kafka Consumer with Retry ===
# def connect_kafka_consumer():
#     """
#     Establishes and returns a KafkaConsumer connection.
#     Retries indefinitely if Kafka brokers are not available.
#     """
#     if not KAFKA_BOOTSTRAP_SERVERS:
#         print("Error: KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
#         sys.exit(1)

#     while True:
#         try:
#             consumer = KafkaConsumer(
#                 KAFKA_TOPIC,
#                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                 group_id=KAFKA_GROUP_ID,
#                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#                 auto_offset_reset='earliest', # Start reading from the earliest available offset if no committed offset
#                 enable_auto_commit=True       # Automatically commit offsets
#             )
#             print("Successfully connected to Kafka (consumer).")
#             return consumer
#         except Exception as e:
#             print(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
#             time.sleep(5)

# # === Save Record to MinIO ===
# def save_record_to_minio(record):
#     """
#     Saves a single macroeconomic data record as a Parquet file to MinIO,
#     partitioned by series alias and year.
#     """
#     try:
#         alias = record["series"]
#         date_str = record["date"]
#         value = record["value"]

#         date_obj = datetime.strptime(date_str, "%Y-%m-%d")
#         year = date_obj.year
#         filename = f"{alias}_{date_str}.parquet"
        
#         # Path in MinIO: bucket/series_alias/year/filename.parquet
#         s3_path = f"{MINIO_BUCKET}/{alias}/{year}/{filename}"

#         # Create a DataFrame for the single record
#         df = pd.DataFrame([{
#             "series": alias,
#             "date": date_str,
#             "value": value
#         }])

#         # Write DataFrame to Parquet directly to MinIO using s3fs
#         with fs.open(s3_path, 'wb') as f:
#             df.to_parquet(f, index=False)

#         print(f"Saved to MinIO: {s3_path}")
#     except Exception as e:
#         print(f"Error saving record to MinIO: {e}")

# # === Main Loop ===
# def main():
#     """
#     Main function to consume messages from Kafka and save them to MinIO.
#     """
#     consumer = connect_kafka_consumer()
#     print(f"Listening for messages on Kafka topic: {KAFKA_TOPIC}...")

#     try:
#         # Iterate over messages from Kafka
#         for message in consumer:
#             record = message.value
#             save_record_to_minio(record)
#     except KeyboardInterrupt:
#         # Handle user interruption (Ctrl+C)
#         print("Interrupted by user.")
#     finally:
#         # Ensure the Kafka consumer is closed when done or interrupted
#         consumer.close()
#         print("Kafka consumer closed.")

# if __name__ == "__main__":
#     main()























import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
import pandas as pd
import s3fs
import sys

# === Kafka Configuration ===
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "h_macrodata"
KAFKA_GROUP_ID = "macro-consumer-group"
CONSUMER_TIMEOUT_MS = 5000  # How long to wait for new messages (in milliseconds)
INACTIVITY_SHUTDOWN_SECONDS = 30 # Shutdown after this many seconds of inactivity

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
    print("Successfully connected to MinIO filesystem via s3fs.")
except Exception as e:
    print(f"Error connecting to MinIO via s3fs: {e}. Please check your credentials and endpoint. Exiting.")
    sys.exit(1)

# === MinIO Bucket Creation ===
def create_minio_bucket_if_not_exists():
    """
    Checks if the MinIO bucket exists, and creates it if it doesn't.
    """
    try:
        if not fs.exists(MINIO_BUCKET):
            fs.mkdir(MINIO_BUCKET)
            print(f"MinIO bucket '{MINIO_BUCKET}' created successfully.")
        else:
            print(f"MinIO bucket '{MINIO_BUCKET}' already exists.")
    except Exception as e:
        print(f"Error ensuring MinIO bucket exists: {e}. Exiting.")
        sys.exit(1)

# Ensure bucket exists before proceeding
create_minio_bucket_if_not_exists()

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
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=CONSUMER_TIMEOUT_MS  # Timeout for message consumption
            )
            print("Successfully connected to Kafka (consumer).")
            return consumer
        except Exception as e:
            print(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
            time.sleep(5)

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

def main():
    """
    Main function to consume messages from Kafka and save them to MinIO.
    Handles consumer shutdown based on inactivity.
    """
    consumer = connect_kafka_consumer()
    print(f"Listening for messages on Kafka topic: {KAFKA_TOPIC}...")

    last_message_time = time.time()
    messages_processed = 0

    try:
        while True:
            message_received_in_batch = False
            # Poll for messages with a timeout
            messages = consumer.poll(timeout_ms=CONSUMER_TIMEOUT_MS)

            if messages:
                for tp, consumer_records in messages.items():
                    for record in consumer_records:
                        save_record_to_minio(record.value)
                        messages_processed += 1
                        last_message_time = time.time() # Update time whenever a message is processed
                        message_received_in_batch = True

            # If no messages were received in the current poll and we've been inactive for a while
            if not message_received_in_batch and (time.time() - last_message_time) > INACTIVITY_SHUTDOWN_SECONDS:
                print(f"No messages received for {INACTIVITY_SHUTDOWN_SECONDS} seconds. Assuming data stream has ended or is inactive. Shutting down.")
                break # Exit the loop to shut down

            if messages_processed == 0 and (time.time() - last_message_time) > INACTIVITY_SHUTDOWN_SECONDS:
                print("No data received since startup and inactivity threshold reached. Shutting down to avoid indefinite wait.")
                break # Exit if no messages ever arrived and we're past the inactivity threshold

    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        consumer.close()
        print("Kafka consumer closed.")
        print(f"Total messages processed: {messages_processed}")

if __name__ == "__main__":
    main()