import os
import requests
import pandas as pd
import time
from kafka import KafkaProducer
import json
from datetime import datetime

# Serie FRED e nomi alias
series_dict = {
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
}

api_key = "119c5415679f73cb0da3b62e9c2a534d"
base_url = "https://api.stlouisfed.org/fred/series/observations"

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
topic = "macrodata_daily"

# Set per tracciare le date già inviate per ciascuna serie
sent_dates = {alias: set() for alias in series_dict.values()}

# 🔁 Loop infinito (una richiesta ogni 6 ore per esempio)
while True:
    print(f"🕒 Avvio ciclo alle {datetime.now()}...\n")
    
    for series_id, alias in series_dict.items():
        print(f"📡 Scarico {alias} ({series_id})...")
        url = f"{base_url}?series_id={series_id}&api_key={api_key}&file_type=json"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            observations = data["observations"]
            df = pd.DataFrame(observations)

            # Pulizia
            df = df[["date", "value"]]
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df.rename(columns={"value": "value"}, inplace=True)

            # Invia solo le date non ancora inviate
            for _, row in df.iterrows():
                date_str = row["date"].strftime("%Y-%m-%d")
                if date_str not in sent_dates[alias]:
                    msg = {
                        "series": alias,
                        "date": date_str,
                        "value": row["value"]
                    }
                    producer.send(topic, value=msg)
                    sent_dates[alias].add(date_str)
                    print(f"📤 Inviato {alias} {date_str}: {row['value']}")
        else:
            print(f"❌ Errore {response.status_code} per {alias}")

    print("\n✅ Ciclo completato. Pausa di 6 ore...\n")
    time.sleep(6 * 60 * 60)  # 6 ore
