from alpaca.data.live import StockDataStream
from datetime import datetime, timedelta
from kafka import KafkaProducer
import time
import pytz
import pandas as pd
import nest_asyncio
import asyncio
import json
import random
import os
import psycopg2

# Apply nest_asyncio for compatibility in embedded environments (e.g., notebooks).
nest_asyncio.apply()

# --- Configuration ---

# Alpaca API credentials. Set as environment variables for security.
API_KEY = os.getenv("API_KEY_ALPACA")
API_SECRET = os.getenv("API_SECRET_ALPACA")

# Kafka broker and topic.
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = 'stock_trades'

# PostgreSQL database connection details for fetching tickers.
# These must be set as environment variables.
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# New York timezone for market hour determination.
ny_timezone = pytz.timezone("America/New_York")

# --- Database Operations ---

def fetch_tickers_from_db():
    """
    Retrieves active stock tickers and their avg_pred_price from the PostgreSQL database.
    """
    max_retries = 10
    delay = 5

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute("SELECT ticker, avg_pred_price FROM companies_info WHERE is_active = TRUE AND avg_pred_price IS NOT NULL;")
            result = cursor.fetchall()
            cursor.close()
            conn.close()

            if not result:
                print("No tickers with avg_pred_price found.")
                return {}

            ticker_price_map = {row[0]: float(row[1]) for row in result if row[0] and row[1]}
            print(f"Loaded {len(ticker_price_map)} tickers with avg_pred_price from DB.")
            return ticker_price_map

        except Exception as e:
            print(f"Database unavailable, retrying in {delay * (attempt + 1)} seconds... ({e})")
            time.sleep(delay * (attempt + 1))

    print("Failed to connect to database after multiple attempts. Exiting.")
    exit(1)


# Fetch tickers on script initialization.
top_tickers = fetch_tickers_from_db()
if not top_tickers:
    print("No tickers available from DB. Exiting.")
    exit(1)

# --- Kafka Operations ---

def connect_kafka():
    """Establishes and returns a Kafka producer connection, with retries."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka.")
            return producer
        except Exception as e:
            print(f"Kafka unavailable, retrying in 5 seconds... ({e})")
            time.sleep(5)

# Initialize Kafka producer.
producer = connect_kafka()

# --- Stock Data Generation & Streaming ---

# Initialize the last known price for each ticker for random data generation.
ticker_price_map = fetch_tickers_from_db()
top_tickers = list(ticker_price_map.keys())
last_prices = {ticker: ticker_price_map[ticker] for ticker in top_tickers}

# Initialize Alpaca StockDataStream.
stream = StockDataStream(API_KEY, API_SECRET)
# Flag to track the active state of the Alpaca stream.
stream_running = False

async def handle_trade_data(data):
    """
    Callback for live trade data from Alpaca.
    Formats the data and sends it to Kafka.
    """
    row = {
        "ticker": data.symbol,
        "timestamp": pd.to_datetime(data.timestamp, utc=True).isoformat(),
        "price": data.price,
        "size": data.size,
        "exchange": data.exchange
    }
    print(f"Kafka (Alpaca): {row}")
    producer.send(KAFKA_TOPIC, value=row)
    producer.flush()

async def generate_random_trade_data():
    """
    Generates simulated stock trade data for all tickers.
    Simulates price and size fluctuations and sends data to Kafka.
    """
    global last_prices
    current_time_utc = datetime.now(pytz.utc).isoformat()

    for ticker in top_tickers:
        price_change = random.uniform(-0.1, 0.1)
        new_price = last_prices[ticker] + price_change
        if new_price < 10: # Ensure price doesn't drop too low
            new_price = 10 + random.uniform(0, 1)
        last_prices[ticker] = new_price
        size = float(random.randint(1, 500))

        row = {
            "ticker": ticker,
            "timestamp": current_time_utc,
            "price": round(new_price, 2),
            "size": size,
            "exchange": "RANDOM" # Indicate random data source
        }
        print(f"Kafka (Random): {row}")
        producer.send(KAFKA_TOPIC, value=row)
    producer.flush()

# --- Market Hours Logic ---

def is_market_open():
    """
    Checks if the New York stock market is currently open (9:30 AM - 4:00 PM ET, weekdays).
    """
    current_time_ny = datetime.now(ny_timezone)
    market_open_time = current_time_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = current_time_ny.replace(hour=16, minute=0, second=0, microsecond=0)

    if current_time_ny.weekday() >= 5: # Saturday or Sunday
        return False
    return market_open_time <= current_time_ny < market_close_time

# --- Main Execution Loop ---

async def main():
    """
    Main asynchronous function. Manages Alpaca stream during market hours
    and generates random data when the market is closed.
    """
    global stream_running

    # Subscribe to live trade data for all tickers on startup.
    for symbol in top_tickers:
        stream.subscribe_trades(handle_trade_data, symbol)

    print("Script started in infinite mode, monitoring market hours.")

    while True:
        if is_market_open():
            if not stream_running:
                print("Market open. Starting Alpaca streaming...")
                asyncio.create_task(stream.run()) # Run stream in background
                stream_running = True
            await asyncio.sleep(1) # Allow Alpaca stream to process.
        else:
            if stream_running:
                print("Market closed. Stopping Alpaca streaming and switching to random data...")
                await stream.stop() # Stop Alpaca stream
                stream_running = False
            await generate_random_trade_data()
            await asyncio.sleep(1) # Wait one second before next data cycle.

if __name__ == "__main__":
    asyncio.run(main())