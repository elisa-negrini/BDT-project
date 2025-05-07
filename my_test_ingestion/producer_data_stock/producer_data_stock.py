import pandas as pd
import asyncio
import websockets
import json
import os
from datetime import datetime, timedelta, timezone
import nest_asyncio
from kafka import KafkaProducer

# Applica patch per permettere asyncio in ambienti Jupyter o simili
nest_asyncio.apply()

# Configurazione Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Chiave API di Finnhub (modifica se serve)
FINNHUB_API_KEY = "d056jb9r01qoigrsmf5gd056jb9r01qoigrsmf60"

# Tickers S&P 500
url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
tickers = pd.read_csv(url)['Symbol'].tolist()

# WebSocket endpoint di Finnhub
FINNHUB_WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

# Storage in memoria
df_all_data = pd.DataFrame()
topic = "stock market"

# Stream dei dati da Finnhub e invio a Kafka
async def stream_finnhub(duration_seconds=60):
    global df_all_data

    async with websockets.connect(FINNHUB_WS_URL) as ws:
        # Subscribe ai tickers
        for symbol in tickers:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            print(f"📡 Subscribed to {symbol}")


        end_time = datetime.now(timezone.utc) + timedelta(seconds=duration_seconds)

        try:
            while datetime.now(timezone.utc) < end_time:
                message = await ws.recv()
                data = json.loads(message)

                if data.get("type") == "trade":
                    for trade in data.get("data", []):
                        row = {
                            "Ticker": trade["s"],
                            "Timestamp": pd.to_datetime(trade["t"], unit="ms", utc=True).isoformat(),
                            "Price": trade["p"],
                            "Size": trade["v"]
                        }
                        # Aggiungi al DataFrame
                        df_all_data = pd.concat([df_all_data, pd.DataFrame([row])], ignore_index=True)
                        print("📈 Trade ricevuto:", row)

                        # Invia a Kafka
                        producer.send(topic, value=row)
        finally:
            
            print("✅ Streaming terminato.")
            await ws.close()

# Esegui lo streaming (modifica la durata se vuoi)
if __name__ == "__main__":
    asyncio.run(stream_finnhub(duration_seconds=10))

