import pandas as pd
import asyncio
import websockets
import json
import os
from datetime import datetime, timedelta
import nest_asyncio
from kafka import KafkaProducer

nest_asyncio.apply()

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

FINNHUB_API_KEY = "d056jb9r01qoigrsmf5gd056jb9r01qoigrsmf60"

# S&P 500 tickers (limit to first 5 for demo)
url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
tickers = pd.read_csv(url)['Symbol'].tolist()

# WebSocket endpoint
FINNHUB_WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

# In-memory storage
df_all_data = pd.DataFrame()

# Create directory
os.makedirs("dati_stream", exist_ok=True)

async def save_data_periodically(interval_sec=60):
    global df_all_data
    while True:
        await asyncio.sleep(interval_sec)
        if not df_all_data.empty:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            path = f"dati_stream/finnhub_stream_{timestamp}.csv"
            df_all_data.to_csv(path, index=False)
            print(f"💾 Salvato {len(df_all_data)} righe in {path}")

async def stream_finnhub(duration_seconds=60):
    global df_all_data

    async with websockets.connect(FINNHUB_WS_URL) as ws:
        # Subscribe to tickers
        for symbol in tickers:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            print(f"📡 Subscribed to {symbol}")

        # Start saving data
        save_task = asyncio.create_task(save_data_periodically(30))

        end_time = datetime.utcnow() + timedelta(seconds=duration_seconds)

        try:
            while datetime.utcnow() < end_time:
                message = await ws.recv()
                data = json.loads(message)

                if data.get("type") == "trade":
                    for trade in data.get("data", []):
                        row = {
                            "Ticker": trade["s"],
                            "Timestamp": pd.to_datetime(trade["t"], unit="ms", utc=True),
                            "Price": trade["p"],
                            "Size": trade["v"]
                        }
                        df_all_data = pd.concat([df_all_data, pd.DataFrame([row])], ignore_index=True)
                        print(df_all_data.tail(1))
        finally:
            await ws.close()
            save_task.cancel()
            print("✅ Streaming finished.")

# Run the stream
await stream_finnhub(duration_seconds=60)
