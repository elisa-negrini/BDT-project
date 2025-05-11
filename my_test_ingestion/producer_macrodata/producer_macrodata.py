import os
import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer
import pandas as pd

# Kafka Config
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'fred_data'

# FRED API Config
API_KEY = "119c5415679f73cb0da3b62e9c2a534d"
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Serie FRED
series_dict = {
    "GDP": "gdp_nominal",
    "GDPC1": "gdp_real",
    "A191RL1Q225SBEA": "gdp_qoq",
    "CPIAUCSL": "cpi",
    "CPILFESL": "cpi_core",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment",
    "UMCSENT": "sentiment"
}

series_ids = list(series_dict.keys())

# Set iniziali per tenere traccia dei timestamp già visti
seen_timestamps = {alias: set() for alias in series_dict.values()}

# Fissa la data di partenza a oggi, eseguito una sola volta
today = datetime.today().strftime('%Y-%m-%d')

# Connessione a Kafka con retry
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connesso a Kafka.")
            return producer
        except Exception as e:
            print(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
            time.sleep(5)

producer = connect_kafka()

# Funzione per fetch e invio
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
                        print(f"📤 Inviato: {payload}")
                        producer.send(KAFKA_TOPIC, value=payload)
            else:
                print(f"❌ Errore API {series_id} - status {response.status_code}")
        except Exception as e:
            print(f"❌ Errore durante la richiesta per {series_id}: {e}")

# Loop continuo con sleep di 1 ora
while True:
    print("🔄 Avvio fetch dati FRED...")
    fetch_and_send()
    print("🕒 Attendo 1 ora...")
    time.sleep(3600)
