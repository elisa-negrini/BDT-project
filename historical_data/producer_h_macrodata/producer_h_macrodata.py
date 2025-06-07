import json
import os
import time
import requests
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import sys

# === CONFIGURATION ===
API_KEY = os.getenv("API_KEY_FRED")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
KAFKA_TOPIC = "h_macrodata"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

series_dict = {
    "GDPC1": "gdp_real",
    "CPIAUCSL": "cpi",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment",
}

# === Kafka Producer Connection with Retry Logic ===
def connect_kafka_producer():
    """Attempts to connect to Kafka. Retries indefinitely if brokers are not available."""
    if not KAFKA_BOOTSTRAP_SERVERS:
        print("Error: KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
        sys.exit(1)

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
            print(f"An unexpected error occurred during Kafka connection: {e}. Retrying in 5 seconds...")
            time.sleep(5)

producer = connect_kafka_producer()

# === Fetch and Send Data Function ===
def fetch_and_send():
    """Fetches macroeconomic data from the FRED API for each series, processes it, and sends it to Kafka."""
    if not API_KEY:
        print("Error: API_KEY_FRED environment variable is not set. Please provide a FRED API key.")
        return

    for series_id, alias in series_dict.items():
        url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json"
        print(f"Downloading {alias} data...")
        try:
            response = requests.get(url)
            if response.status_code == 200:
                observations = response.json().get("observations", [])
                
                df = pd.DataFrame(observations)
                df = df[df["value"] != "."]
                df["date"] = pd.to_datetime(df["date"])
                df = df[df["date"].dt.year >= 2020]
                df["value"] = df["value"].astype(float)

                for _, row in df.iterrows():
                    record = {
                        "series": alias,
                        "date": row["date"].strftime("%Y-%m-%d"),
                        "value": row["value"]
                    }
                    producer.send(KAFKA_TOPIC, record)

                print(f"Sent {len(df)} records for {alias}.")
            else:
                print(f"FRED API Error for {series_id}: HTTP Status {response.status_code}.")
        except requests.exceptions.RequestException as req_err:
            print(f"Request error for {series_id}: {req_err}")
        except json.JSONDecodeError as json_err:
            print(f"JSON decode error for {series_id}: {json_err}. Response content: {response.text[:200]}...")
        except Exception as e:
            print(f"An unexpected error occurred for {series_id}: {e}")

# === Main Execution Block ===
if __name__ == "__main__":
    fetch_and_send()
    producer.flush()
    producer.close()