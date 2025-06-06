import os
import time
import json
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
import pandas as pd

# Kafka configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = 'macrodata'

# FRED API configuration
API_KEY = os.getenv("API_KEY_FRED")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# FRED series mapping
series_dict = {
    "GDPC1": "gdp_real",
    "CPIAUCSL": "cpi",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment"
}

series_ids = list(series_dict.keys())

# Track previously seen timestamps for each alias
seen_timestamps = {alias: set() for alias in series_dict.values()}

# Fix reference date to today (once at startup)
today = datetime.today().strftime('%Y-%m-%d')

# Kafka connection with retry logic
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer connected successfully.")
            return producer
        except Exception as e:
            print(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

producer = connect_kafka()

# Initialization: fetch and send most recent value for each series
def initialize_seen_timestamps():
    print("Initializing: retrieving latest values for all FRED series...")

    for series_id, alias in series_dict.items():
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
            print(f"Error during init for {series_id}: {e}")

# Fetch and send new values since the last check
def fetch_and_send():
    for series_id, alias in series_dict.items():
        url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json&observation_start={today}"
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

# Run init
initialize_seen_timestamps()

# Continuous loop with 1-hour interval
while True:
    print("Fetching new FRED data...")
    fetch_and_send()
    print("Sleeping for 1 hour...")
    time.sleep(3600)