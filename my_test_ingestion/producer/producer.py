""" from kafka import KafkaProducer
import time
import json
import random
from datetime import datetime

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Stocks we want to simulate
tickers = ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA']

# Initialize random starting prices
prices = {ticker: random.uniform(100, 500) for ticker in tickers}

# Kafka topic
topic = "test_topic"

count = 0
while True:
    # Randomly pick one stock
    ticker = random.choice(tickers)

    # Simulate a small random price change
    change = random.uniform(-1, 1)  # -1% to +1%
    prices[ticker] *= (1 + change / 100)

    # Build message
    message = {
        "ticker": ticker,
        "price": round(prices[ticker], 2),
        "timestamp": datetime.utcnow().isoformat()
    }

    # Send to Kafka
    producer.send(topic, message)
    print(f"Sent: {message}")

    time.sleep(1)  # Send one message per second
    count += 1
 """
# Simulates 5 different stocks.
#
# Each stock price randomly moves up or down by about ±1% every second.
#
# Sends a clean JSON message to Kafka like:
# {"ticker": "AAPL", "price": 152.35, "timestamp": "2025-04-28T08:10:00.123456"}

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
from kafka import KafkaProducer
import json
import time

# Step 1: Leggi la lista dei simboli S&P 500
s_and_p_500 = pd.read_csv("https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv")
symbols = s_and_p_500['Symbol'].tolist()

# Step 2: Connessione Alpaca
client = StockHistoricalDataClient(
    "AKP52FXPKKRCHO0R1QJ5",
    "cvdzZzgt7QZHjF5n1vNFL4wUKo6Rq0Hy6gLnCdVt"
)

# Step 3: Richiesta dati storici
request_params = StockBarsRequest(
    symbol_or_symbols=symbols,
    timeframe=TimeFrame.Day,
    start=datetime(2024, 4, 20),
    end=datetime(2025, 4, 25)
)

bars = client.get_stock_bars(request_params)
df = bars.df
df = bars.df.reset_index()


if df.empty:
    raise ValueError("Nessun dato valido ricevuto da Alpaca.")

# Step 4: Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "test_topic"

# Step 5: Invia dati a Kafka, riga per riga
for idx, row in df.iterrows():
    message = {
        "ticker": row["symbol"],
        "price": round(row["close"], 2),
        "timestamp": row["timestamp"].isoformat()
    }
    producer.send(topic, message)
    print(f"Sent: {message}")
    time.sleep(1)  # Simula flusso continuo
