from kafka import KafkaProducer
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

# Simulates 5 different stocks.
#
# Each stock price randomly moves up or down by about ±1% every second.
#
# Sends a clean JSON message to Kafka like:
# {"ticker": "AAPL", "price": 152.35, "timestamp": "2025-04-28T08:10:00.123456"}