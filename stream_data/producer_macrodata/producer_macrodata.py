import os
import time
import json
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
import pandas as pd

# ====== Configuration ======

# Kafka broker and topic.
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = 'macrodata'

# FRED API key and base URL.
API_KEY = os.getenv("API_KEY_FRED")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Mapping of FRED series IDs to human-readable aliases.
SERIES_MAPPING = {
    "GDPC1": "gdp_real",
    "CPIAUCSL": "cpi",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment"
}

# List of FRED series IDs to fetch.
SERIES_IDS = list(SERIES_MAPPING.keys())

# Track previously sent timestamps for each series alias to avoid duplicates.
seen_timestamps = {alias: set() for alias in SERIES_MAPPING.values()}

# Fix the reference date to today for subsequent data fetches.
TODAY = datetime.today().strftime('%Y-%m-%d')

# ====== Kafka Operations ======
def connect_kafka():
    """Establishes and returns a Kafka producer, with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer connected successfully.")
            return producer
        except Exception as e:
            print(f"Kafka unavailable, retrying in 5 seconds... ({e})")
            time.sleep(5)

producer = connect_kafka()

# ====== FRED Data Fetching and Sending ======
def initialize_seen_timestamps():
    """
    Initializes the 'seen_timestamps' set by fetching the latest observation
    for each FRED series and sending it to Kafka.
    This ensures that the script starts with the most recent data.
    """
    print("Initializing: retrieving latest values for all FRED series...")

    for series_id, alias in SERIES_MAPPING.items():
        url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json&sort_order=desc&limit=1"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json().get("observations", [])
                for obs in data:
                    date = obs["date"]
                    value = obs["value"]

                    if value != ".":
                        seen_timestamps[alias].add(date)
                        payload = {
                            "series_id": series_id,
                            "alias": alias,
                            "date": date,
                            "value": float(value)
                        }
                        print(f"Sent (init): {payload}")
                        producer.send(KAFKA_TOPIC, value=payload)
                    else:
                        print(f"Missing value for {series_id} on {date}")
            else:
                print(f"API error during init for {series_id} - status {response.status_code}")
        except Exception as e:
            print(f"Request error during init for {series_id}: {e}")

def fetch_and_send():
    """Fetches new macroeconomic data from FRED and sends only new observations to Kafka."""
    for series_id, alias in SERIES_MAPPING.items():
        url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json&observation_start={TODAY}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json().get("observations", [])
                for obs in data:
                    date = obs["date"]
                    value = obs["value"]

                    if date not in seen_timestamps[alias] and value != ".":
                        seen_timestamps[alias].add(date)
                        payload = {
                            "series_id": series_id,
                            "alias": alias,
                            "date": date,
                            "value": float(value)
                        }
                        print(f"Sent: {payload}")
                        producer.send(KAFKA_TOPIC, value=payload)
            else:
                print(f"API error for {series_id} - status {response.status_code}")
        except Exception as e:
            print(f"Request error for {series_id}: {e}")

# ====== Main Execution Loop ======

# Perform initial fetch to populate seen_timestamps and send latest data.
initialize_seen_timestamps()

# Continuous loop to periodically fetch and send new data.
while True:
    print("Fetching new FRED data...")
    fetch_and_send()
    print("Sleeping for 1 hour...")
    time.sleep(3600) # Sleep for 1 hour (3600 seconds)