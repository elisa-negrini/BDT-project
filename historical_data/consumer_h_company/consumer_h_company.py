# #!/usr/bin/env python3
# import os
# import json
# import time
# import pandas as pd
# import boto3
# from io import BytesIO
# from kafka import KafkaConsumer
# import logging
# import sys

# # === LOGGING ===
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("KafkaToMinio")

# # === CONFIGURATION ===
# KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# KAFKA_TOPIC = "h_company"
# MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
# MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
# MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
# MINIO_BUCKET ="company-fundamentals"

# # === KAFKA CONSUMER FUNCTION ===
# def connect_kafka_consumer():
#     """
#     Establishes and returns a KafkaConsumer connection.
#     Retries indefinitely if Kafka brokers are not available.
#     """
#     if not KAFKA_BOOTSTRAP_SERVERS:
#         logger.critical("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
#         sys.exit(1)

#     while True:
#         try:
#             consumer = KafkaConsumer(
#                 KAFKA_TOPIC,
#                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#                 auto_offset_reset='earliest', # Start reading from the beginning of the topic if no committed offset is found
#                 enable_auto_commit=True,      # Automatically commit offsets
#                 consumer_timeout_ms=10000     # Timeout after 10 seconds if no message is received
#             )
#             logger.info("Successfully connected to Kafka (consumer).")
#             return consumer
#         except Exception as e:
#             logger.warning(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
#             time.sleep(5)

# def ensure_bucket_exists(bucket_name, endpoint, access_key, secret_key):
#     """
#     Ensures that the specified MinIO/S3 bucket exists. Creates it if it doesn't.
#     """
#     if not all([endpoint, access_key, secret_key]):
#         logger.critical("MinIO/S3 environment variables (ENDPOINT_URL, ACCESS_KEY, SECRET_KEY) are not set. Exiting.")
#         sys.exit(1)

#     s3 = boto3.client(
#         's3',
#         endpoint_url=endpoint,
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key
#     )
#     try:
#         buckets = s3.list_buckets()
#         if not any(b['Name'] == bucket_name for b in buckets.get('Buckets', [])):
#             s3.create_bucket(Bucket=bucket_name)
#             logger.info(f"Bucket '{bucket_name}' created.")
#         else:
#             logger.info(f"Bucket '{bucket_name}' already exists.")
#     except Exception as e:
#         logger.error(f"Error checking/creating bucket: {e}")
#         raise # Re-raise the exception to stop execution if bucket cannot be assured

# # === START CONSUMER AND COLLECT RECORDS ===
# consumer = connect_kafka_consumer()
# logger.info("Listening for messages on Kafka...")

# records = []
# max_retries = 20
# retry_delay = 5

# try:
#     for i in range(max_retries):
#         try:
#             logger.info(f"Attempt {i+1}/{max_retries}: polling for messages...")
#             msg_pack = consumer.poll(timeout_ms=5000)  # Poll con timeout di 5 secondi
#             any_message = False
#             for tp, messages in msg_pack.items():
#                 for message in messages:
#                     records.append(message.value)
#                     logger.info(f"Received record for symbol: {message.value.get('symbol', 'N/A')}")
#                     any_message = True
#             if any_message:
#                 break  # Esci dal ciclo se almeno un messaggio è stato ricevuto
#         except Exception as e:
#             logger.error(f"Error during polling: {e}")
#             time.sleep(retry_delay)
# finally:
#     consumer.close()

# if not records:
#     logger.warning("No data received from Kafka within the timeout period. Exiting.")
#     sys.exit(0)

# # === CONVERT TO PANDAS DATAFRAME ===
# df = pd.DataFrame(records)

# # Filter out rows where 'calendarYear' is missing, as it's critical for partitioning
# df = df[df['calendarYear'].notna()]

# # Check for essential columns required for processing
# required_columns = {'symbol', 'calendarYear'}
# if not required_columns.issubset(df.columns):
#     logger.error(f"Missing required columns in DataFrame: {required_columns - set(df.columns)}. Exiting.")
#     sys.exit(1)

# # Ensure calendarYear is integer for consistent partitioning
# df['calendarYear'] = df['calendarYear'].astype(int)

# # === WRITE TO MINIO ===
# ensure_bucket_exists(MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

# session = boto3.session.Session()
# s3 = session.client(
#     service_name='s3',
#     endpoint_url=MINIO_ENDPOINT,
#     aws_access_key_id=MINIO_ACCESS_KEY,
#     aws_secret_access_key=MINIO_SECRET_KEY
# )

# logger.info(f"Starting to upload data to MinIO bucket '{MINIO_BUCKET}'...")
# uploaded_files_count = 0
# try:
#     # Group DataFrame by 'symbol' and 'calendarYear' to create partitioned Parquet files
#     for (symbol, year), group_df in df.groupby(['symbol', 'calendarYear']):
#         buffer = BytesIO()
#         group_df.to_parquet(buffer, index=False) # Write group to in-memory buffer
#         buffer.seek(0) # Reset buffer position to the beginning
        
#         key = f"{symbol}/{year}.parquet" # Define object key (path in bucket)
#         s3.upload_fileobj(buffer, MINIO_BUCKET, key) # Upload the buffer content
#         logger.info(f"Uploaded: {key}")
#         uploaded_files_count += 1
# except Exception as e:
#     logger.error(f"An error occurred during file upload to MinIO: {e}")
#     sys.exit(1)

# logger.info(f"Successfully saved {uploaded_files_count} files to MinIO.")




















#!/usr/bin/env python3
import os
import json
import time
import pandas as pd
import boto3
from io import BytesIO
from kafka import KafkaConsumer
import logging
import sys

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("KafkaToMinio")

# === CONFIGURATION ===
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "h_company"
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET ="company-fundamentals"

# === CONSUMER BEHAVIOR CONFIGURATION ===
# How long the consumer waits for new messages in each poll before returning (in milliseconds)
CONSUMER_POLL_TIMEOUT_MS = 5000
# How long the consumer waits for any messages before deciding to shut down due to inactivity (in seconds)
INACTIVITY_SHUTDOWN_SECONDS = 100

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
                consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS # Timeout for message consumption
            )
            logger.info("Successfully connected to Kafka (consumer).")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka not available (consumer), retrying in 5 seconds... ({e})")
            time.sleep(5)

# === MINIO BUCKET ENSURE FUNCTION ===
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
        # It's more robust to try to create and catch the error if it already exists,
        # or use head_bucket if permissions allow. list_buckets can be less efficient
        # for a simple check.
        # For simplicity, we'll stick to list_buckets for now as it's often more compatible
        # across various S3-compatible services regarding permissions.
        
        # Check if the bucket exists by listing and filtering
        response = s3.list_buckets()
        found = False
        for bucket in response.get('Buckets', []):
            if bucket['Name'] == bucket_name:
                found = True
                break
        
        if not found:
            s3.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        logger.error(f"Error checking/creating bucket '{bucket_name}': {e}")
        # Re-raise the exception to stop execution if bucket cannot be assured
        sys.exit(1) # Exit critically if bucket operations fail

# --- MAIN EXECUTION ---
def main():
    consumer = connect_kafka_consumer()
    logger.info(f"Listening for messages on Kafka topic: {KAFKA_TOPIC}...")

    # Ensure MinIO bucket exists before starting to process data
    ensure_bucket_exists(MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    all_records = []
    last_message_time = time.time() # Initialize last message time
    messages_processed_count = 0

    try:
        while True:
            # Poll for messages. The consumer_timeout_ms will make poll return
            # an empty dict if no messages are received within the timeout.
            msg_pack = consumer.poll(timeout_ms=CONSUMER_POLL_TIMEOUT_MS)

            current_poll_received_message = False
            if msg_pack:
                for tp, messages in msg_pack.items():
                    for message in messages:
                        all_records.append(message.value)
                        logger.info(f"Received record for symbol: {message.value.get('symbol', 'N/A')}")
                        messages_processed_count += 1
                        last_message_time = time.time() # Update time whenever a message is received
                        current_poll_received_message = True
            
            # Check for inactivity. If no messages were received in the current poll
            # AND the time since the last message exceeds the inactivity threshold, shut down.
            if not current_poll_received_message and (time.time() - last_message_time) > INACTIVITY_SHUTDOWN_SECONDS:
                if messages_processed_count == 0:
                    logger.warning(f"No messages received from Kafka and no activity for {INACTIVITY_SHUTDOWN_SECONDS} seconds. Shutting down as no data arrived.")
                else:
                    logger.info(f"No new messages received for {INACTIVITY_SHUTDOWN_SECONDS} seconds. Assuming data stream has ended or is inactive. Initiating shutdown.")
                break # Exit the loop to shut down

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Processing collected records...")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

    if not all_records:
        logger.warning("No data was collected from Kafka. Exiting without writing to MinIO.")
        sys.exit(0)

    logger.info(f"Total messages processed before shutdown: {messages_processed_count}")

    # === CONVERT TO PANDAS DATAFRAME ===
    df = pd.DataFrame(all_records)

    # Filter out rows where 'calendarYear' is missing, as it's critical for partitioning
    original_rows = len(df)
    df = df[df['calendarYear'].notna()]
    if len(df) < original_rows:
        logger.warning(f"Filtered out {original_rows - len(df)} records due to missing 'calendarYear'.")

    # Check for essential columns required for processing
    required_columns = {'symbol', 'calendarYear'}
    if not required_columns.issubset(df.columns):
        logger.error(f"Missing required columns in DataFrame: {required_columns - set(df.columns)}. Exiting.")
        sys.exit(1)

    # Ensure calendarYear is integer for consistent partitioning
    df['calendarYear'] = df['calendarYear'].astype(int)

    # === WRITE TO MINIO ===
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    logger.info(f"Starting to upload {len(df)} records to MinIO bucket '{MINIO_BUCKET}'...")
    uploaded_files_count = 0
    try:
        # Group DataFrame by 'symbol' and 'calendarYear' to create partitioned Parquet files
        # The .values.tolist() is added to group_df to handle cases where a single row df causes issues.
        # However, to_parquet usually handles single-row DFs fine.
        # The primary goal is to write a single parquet file per symbol and year.
        for (symbol, year), group_df in df.groupby(['symbol', 'calendarYear']):
            buffer = BytesIO()
            group_df.to_parquet(buffer, index=False) # Write group to in-memory buffer
            buffer.seek(0) # Reset buffer position to the beginning
            
            key = f"{symbol}/{year}.parquet" # Define object key (path in bucket)
            s3.upload_fileobj(buffer, MINIO_BUCKET, key) # Upload the buffer content
            logger.info(f"Uploaded: s3://{MINIO_BUCKET}/{key}")
            uploaded_files_count += 1
    except Exception as e:
        logger.error(f"An error occurred during file upload to MinIO: {e}")
        sys.exit(1)

    logger.info(f"Successfully saved {uploaded_files_count} files to MinIO.")
    logger.info("Kafka to MinIO process completed.")

if __name__ == "__main__":
    main()