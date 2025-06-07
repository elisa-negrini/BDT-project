#!/usr/bin/env python3
import sys
import os
import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable # Import for specific exception handling

# === CONFIGURATION ===
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "h_company"
PARQUET_FILE = "/app/kp_historical_company/df_company_fundamentals.parquet"

# === KAFKA CONNECTION ===
def connect_kafka():
    """
    Establishes and returns a KafkaProducer connection.
    Retries indefinitely if Kafka brokers are not available.
    """
    if not KAFKA_BOOTSTRAP_SERVERS:
        print("Error: KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
        sys.exit(1) # Using sys.exit for critical configuration errors

    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka.")
            return producer
        except NoBrokersAvailable as e:
            print(f"Kafka brokers not available, retrying in 5 seconds... ({e})")
            time.sleep(5)
        except Exception as e:
            # Catch broader exceptions during connection attempts
            print(f"An unexpected error occurred during Kafka connection: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Initialize Kafka producer
producer = connect_kafka()

# === READ PARQUET FILE ===
try:
    df = pd.read_parquet(PARQUET_FILE)
    print(f"Loaded {len(df)} records from {PARQUET_FILE}.")
except FileNotFoundError:
    print(f"Error: Parquet file not found at {PARQUET_FILE}. Exiting.")
    producer.close()
    sys.exit(1)
except Exception as e:
    print(f"Error reading Parquet file {PARQUET_FILE}: {e}. Exiting.")
    producer.close()
    sys.exit(1)

# === PRODUCE TO KAFKA ===
print(f"Starting to send records to Kafka topic: {KAFKA_TOPIC}")
for index, row in df.iterrows():
    # Convert row to dictionary, dropping NaN values
    record = row.dropna().to_dict()
    
    try:
        # Send the record to Kafka
        producer.send(KAFKA_TOPIC, value=record)
        # Log which record was sent
        print(f"Sent record for Symbol: {record.get('symbol', 'N/A')} - Year: {record.get('calendarYear', 'N/A')}")
    except Exception as e:
        print(f"Error sending record for Symbol: {record.get('symbol', 'N/A')} - Year: {record.get('calendarYear', 'N/A')}: {e}")
        # Depending on criticality, you might want to exit or implement more robust retry logic here

# Ensure all buffered messages are sent to Kafka
producer.flush()
# Close the producer connection
producer.close()

print("Finished sending all records to Kafka.")