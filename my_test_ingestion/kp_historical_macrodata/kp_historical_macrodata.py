import json
import os
import time
import requests
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

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

# === Kafka Producer with Retry ===
def connect_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connessione a Kafka riuscita.")
            return producer
        except NoBrokersAvailable as e:
            print(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
            time.sleep(5)

producer = connect_kafka_producer()

def fetch_and_send():
    for series_id, alias in series_dict.items():
        url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json"
        print(f"⬇️  Scarico {alias}...")
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

                print(f"✅ Inviati {len(df)} record per {alias}")
            else:
                print(f"❌ Errore FRED {series_id}: {response.status_code}")
        except Exception as e:
            print(f"❌ Errore {series_id}: {e}")

if __name__ == "__main__":
    fetch_and_send()
    producer.flush()
    producer.close()


