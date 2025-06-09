# #!/usr/bin/env python3

# import os
# import json
# import logging
# import signal
# import sys
# import time
# from datetime import datetime
# from kafka import KafkaConsumer
# import boto3
# import pandas as pd
# import pyarrow.parquet as pq
# import pyarrow as pa
# from io import BytesIO

# # === CONFIGURATION ===
# KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# KAFKA_TOPIC = "h_alpaca"
# KAFKA_GROUP_ID = "consumer_alpaca"

# MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL") 
# MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
# MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
# MINIO_BUCKET = "historical-data"

# # === LOGGING ===
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger("simple-consumer")

# # === SHUTDOWN SIGNAL HANDLING ===
# shutdown_flag = False
# def signal_handler(signum, frame):
#     """Sets a global flag to gracefully shut down the consumer."""
#     global shutdown_flag
#     shutdown_flag = True
#     logger.info("Shutdown requested...")

# signal.signal(signal.SIGINT, signal_handler)
# signal.signal(signal.SIGTERM, signal_handler)

# # === S3 CLIENT INITIALIZATION ===
# # Validate MinIO/S3 configuration before proceeding
# if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET]):
#     logger.critical("One or more MinIO/S3 environment variables (S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY, MINIO_BUCKET) are not set. Exiting.")
#     sys.exit(1)

# s3 = boto3.client(
#     's3',
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY
# )


# def create_minio_bucket():
#     """
#     Attempts to create the specified MinIO bucket if it does not already exist.
#     """
#     try:
#         s3.head_bucket(Bucket=MINIO_BUCKET)
#         logger.info(f"Bucket '{MINIO_BUCKET}' already exists.")
#     except s3.exceptions.ClientError as e:
#         error_code = e.response['Error']['Code']
#         if error_code == '404':
#             # Bucket does not exist, so create it
#             try:
#                 s3.create_bucket(Bucket=MINIO_BUCKET)
#                 logger.info(f"Bucket '{MINIO_BUCKET}' created successfully.")
#             except Exception as create_e:
#                 logger.critical(f"Failed to create bucket '{MINIO_BUCKET}': {create_e}. Exiting.")
#                 sys.exit(1)
#         else:
#             # Another error occurred when checking the bucket
#             logger.critical(f"Error checking bucket '{MINIO_BUCKET}': {e}. Exiting.")
#             sys.exit(1)
#     except Exception as e:
#         # Catch any other unexpected errors during bucket check
#         logger.critical(f"Unexpected error during bucket check for '{MINIO_BUCKET}': {e}. Exiting.")
#         sys.exit(1)

# # Ensure the bucket exists before proceeding
# create_minio_bucket()

# # === KAFKA CONSUMER CONNECTION ===
# def connect_kafka_consumer():
#     """Attempts to connect to Kafka consumer with retry logic until successful."""
#     if not KAFKA_BOOTSTRAP_SERVERS:
#         logger.critical("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
#         sys.exit(1)

#     while True:
#         try:
#             consumer = KafkaConsumer(
#                 KAFKA_TOPIC,
#                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#                 key_deserializer=lambda m: m.decode('utf-8') if m else None,
#                 auto_offset_reset='earliest', # Start consuming from the earliest available offset
#                 enable_auto_commit=True,      # Automatically commit offsets
#                 group_id=KAFKA_GROUP_ID,      # Consumer group ID
#                 consumer_timeout_ms=10000     # Timeout after 10 seconds if no message is received
#             )
#             logger.info("Successfully connected to Kafka (consumer).")
#             return consumer
#         except Exception as e:
#             logger.warning(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
#             time.sleep(5)

# # === SAVE DATA TO MINIO ===
# def save_to_minio(ticker, year, month, date_str, records):
#     """
#     Converts a list of records for a specific day into a Parquet DataFrame
#     and uploads it to MinIO.
#     """
#     if not records:
#         return False

#     try:
#         # Add ticker to each record for a consistent schema if needed, though already in outer payload
#         # This loop might be redundant if 'ticker' is consistently part of each 'data' item,
#         # but safe if 'data' is just the bars without ticker.
#         for r in records:
#             if 'ticker' not in r: # Only add if not already present
#                 r['ticker'] = ticker

#         df = pd.DataFrame(records)
#         df['timestamp'] = pd.to_datetime(df['timestamp']) # Ensure timestamp is datetime object

#         table = pa.Table.from_pandas(df) # Convert Pandas DataFrame to PyArrow Table
#         buf = BytesIO()
#         pq.write_table(table, buf, compression='snappy') # Write Table to Parquet in-memory
#         buf.seek(0) # Reset buffer position to the beginning for upload

#         # Construct MinIO object key (path) for partitioned storage
#         # e.g., ticker/year/month/ticker_date.parquet
#         key = f"{ticker}/{year}/{month:02d}/{ticker}_{date_str}.parquet"
        
#         # Upload the Parquet file to MinIO
#         s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.getvalue())
#         logger.info(f"Saved to MinIO: {key} ({len(df)} records)")
#         return True

#     except Exception as e:
#         logger.error(f"Error saving Parquet for {ticker} {date_str}: {e}")
#         return False

# # === MAIN EXECUTION LOOP ===
# def main():
#     """Main function to consume Kafka messages and store them in MinIO."""
#     logger.info("Starting Kafka to MinIO simplified consumer.")

#     consumer = connect_kafka_consumer()
#     processed_messages = 0
#     errors_encountered = 0

#     max_inactivity_seconds = 60 # Consumer will shut down after this many seconds without messages
#     last_message_time = time.time()

#     try:
#         while not shutdown_flag:
#             try:
#                 # Poll for messages with a timeout to allow checking shutdown_flag
#                 message_pack = consumer.poll(timeout_ms=1000) # Poll for 1 second

#                 if message_pack:
#                     last_message_time = time.time() # Reset inactivity timer if messages arrive

#                 for tp, messages in message_pack.items():
#                     for message in messages:
#                         if shutdown_flag: # Check flag again after getting messages
#                             break

#                         try:
#                             payload = message.value
#                             ticker = payload.get('ticker')
#                             year = payload.get('year')
#                             month = payload.get('month')
#                             date_str = payload.get('date')
#                             records = payload.get('data')

#                             if not all([ticker, year, month, date_str, records]):
#                                 logger.warning(f"Skipping malformed message: Missing key data in payload {payload}")
#                                 errors_encountered += 1
#                                 continue

#                             if save_to_minio(ticker, year, month, date_str, records):
#                                 processed_messages += 1
#                             else:
#                                 errors_encountered += 1

#                         except Exception as e:
#                             logger.error(f"Error processing message: {e}", exc_info=True)
#                             errors_encountered += 1
#                     if shutdown_flag:
#                         break # Break from inner loop if shutdown requested

#                 # Check for inactivity and initiate shutdown if no messages for a while
#                 if time.time() - last_message_time > max_inactivity_seconds:
#                     logger.info(f"No messages received for {max_inactivity_seconds} seconds. Shutting down.")
#                     break # Exit main loop

#             except Exception as e:
#                 logger.error(f"Error in polling loop: {e}", exc_info=True)
#                 errors_encountered += 1
#                 break # Break from main loop on critical polling error

#     except KeyboardInterrupt:
#         logger.info("User interruption detected.")
#     finally:
#         consumer.close() # Ensure Kafka consumer is closed
#         logger.info(f"Consumer terminated. Processed messages: {processed_messages}, Errors: {errors_encountered}")

# if __name__ == "__main__":
#     main()










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

# === CONFIGURATION ===
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "h_alpaca"
KAFKA_GROUP_ID = "consumer_alpaca"

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL") 
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET = "historical-data"

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("simple-consumer")

# === SHUTDOWN SIGNAL HANDLING ===
shutdown_flag = False
def signal_handler(signum, frame):
    """Sets a global flag to gracefully shut down the consumer."""
    global shutdown_flag
    shutdown_flag = True
    logger.info("Shutdown requested...")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# === S3 CLIENT INITIALIZATION ===
# Validate MinIO/S3 configuration before proceeding
if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET]):
    logger.critical("One or more MinIO/S3 environment variables (S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY, MINIO_BUCKET) are not set. Exiting.")
    sys.exit(1)

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

def create_minio_bucket():
    """
    Attempts to create the specified MinIO bucket if it does not already exist.
    """
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        logger.info(f"Bucket '{MINIO_BUCKET}' already exists.")
    except s3.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket does not exist, so create it
            try:
                s3.create_bucket(Bucket=MINIO_BUCKET)
                logger.info(f"Bucket '{MINIO_BUCKET}' created successfully.")
            except Exception as create_e:
                logger.critical(f"Failed to create bucket '{MINIO_BUCKET}': {create_e}. Exiting.")
                sys.exit(1)
        else:
            # Another error occurred when checking the bucket
            logger.critical(f"Error checking bucket '{MINIO_BUCKET}': {e}. Exiting.")
            sys.exit(1)
    except Exception as e:
        # Catch any other unexpected errors during bucket check
        logger.critical(f"Unexpected error during bucket check for '{MINIO_BUCKET}': {e}. Exiting.")
        sys.exit(1)

# Ensure the bucket exists before proceeding
create_minio_bucket()

# === KAFKA CONSUMER CONNECTION ===
def connect_kafka_consumer():
    """Attempts to connect to Kafka consumer with retry logic until successful."""
    if not KAFKA_BOOTSTRAP_SERVERS:
        logger.critical("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
        sys.exit(1)

    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                auto_offset_reset='earliest', # Start consuming from the earliest available offset
                enable_auto_commit=True,      # Automatically commit offsets
                group_id=KAFKA_GROUP_ID,      # Consumer group ID
                consumer_timeout_ms=10000     # Timeout after 10 seconds if no message is received
            )
            logger.info("Successfully connected to Kafka (consumer).")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
            time.sleep(5)

# === SAVE DATA TO MINIO ===
def save_to_minio(ticker, year, month, date_str, records):
    """
    Converts a list of records for a specific day into a Parquet DataFrame
    and uploads it to MinIO.
    """
    if not records:
        return False

    try:
        # Add ticker to each record for a consistent schema if needed, though already in outer payload
        # This loop might be redundant if 'ticker' is consistently part of each 'data' item,
        # but safe if 'data' is just the bars without ticker.
        for r in records:
            if 'ticker' not in r: # Only add if not already present
                r['ticker'] = ticker

        df = pd.DataFrame(records)
        df['timestamp'] = pd.to_datetime(df['timestamp']) # Ensure timestamp is datetime object

        table = pa.Table.from_pandas(df) # Convert Pandas DataFrame to PyArrow Table
        buf = BytesIO()
        pq.write_table(table, buf, compression='snappy') # Write Table to Parquet in-memory
        buf.seek(0) # Reset buffer position to the beginning for upload

        # Construct MinIO object key (path) for partitioned storage
        # e.g., ticker/year/month/ticker_date.parquet
        key = f"{ticker}/{year}/{month:02d}/{ticker}_{date_str}.parquet"
        
        # Upload the Parquet file to MinIO
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.getvalue())
        logger.info(f"Saved to MinIO: {key} ({len(df)} records)")
        return True

    except Exception as e:
        logger.error(f"Error saving Parquet for {ticker} {date_str}: {e}")
        return False

# === MAIN EXECUTION LOOP ===
def main():
    """Main function to consume Kafka messages and store them in MinIO."""
    logger.info("Starting Kafka to MinIO simplified consumer.")

    consumer = connect_kafka_consumer()
    processed_messages = 0
    errors_encountered = 0

    max_inactivity_seconds = 300 # Consumer will shut down after this many seconds without messages
    last_message_time = time.time()

    try:
        while not shutdown_flag:
            try:
                # Poll for messages with a timeout to allow checking shutdown_flag
                message_pack = consumer.poll(timeout_ms=1000) # Poll for 1 second

                if message_pack:
                    last_message_time = time.time() # Reset inactivity timer if messages arrive

                for tp, messages in message_pack.items():
                    for message in messages:
                        if shutdown_flag: # Check flag again after getting messages
                            break

                        try:
                            payload = message.value
                            ticker = payload.get('ticker')
                            year = payload.get('year')
                            month = payload.get('month')
                            date_str = payload.get('date')
                            records = payload.get('data')

                            if not all([ticker, year, month, date_str, records]):
                                logger.warning(f"Skipping malformed message: Missing key data in payload {payload}")
                                errors_encountered += 1
                                continue

                            if save_to_minio(ticker, year, month, date_str, records):
                                processed_messages += 1
                            else:
                                errors_encountered += 1

                        except Exception as e:
                            logger.error(f"Error processing message: {e}", exc_info=True)
                            errors_encountered += 1
                    if shutdown_flag:
                        break # Break from inner loop if shutdown requested

                # Check for inactivity and initiate shutdown if no messages for a while
                if time.time() - last_message_time > max_inactivity_seconds:
                    logger.info(f"No messages received for {max_inactivity_seconds} seconds. Shutting down.")
                    break # Exit main loop

            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
                errors_encountered += 1
                break # Break from main loop on critical polling error

    except KeyboardInterrupt:
        logger.info("User interruption detected.")
    finally:
        consumer.close() # Ensure Kafka consumer is closed
        logger.info(f"Consumer terminated. Processed messages: {processed_messages}, Errors: {errors_encountered}")

if __name__ == "__main__":
    main()