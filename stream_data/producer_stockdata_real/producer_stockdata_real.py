from alpaca.data.live import StockDataStream
from datetime import datetime
from kafka import KafkaProducer
import time
import pytz
import pandas as pd
import nest_asyncio
import asyncio
import json
import os
import psycopg2

# Apply nest_asyncio for compatibility in embedded environments
nest_asyncio.apply()

# ====== Configuration ======
API_KEY = os.getenv("API_KEY_ALPACA")
API_SECRET = os.getenv("API_SECRET_ALPACA")

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = 'stock_trades'

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# New York timezone for market hour determination
ny_timezone = pytz.timezone("America/New_York")

# ====== Database Operations ======
def fetch_tickers_from_db():
    """Retrieves active stock tickers from the PostgreSQL database."""
    max_retries = 10
    delay = 5

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute("SELECT ticker FROM companies_info WHERE is_active = TRUE;")
            result = cursor.fetchall()
            cursor.close()
            conn.close()

            if not result:
                print("No active tickers found.")
                return []

            tickers = [row[0] for row in result if row[0]]
            print(f"Loaded {len(tickers)} tickers from DB.")
            return tickers

        except Exception as e:
            print(f"Database unavailable, retrying in {delay * (attempt + 1)} seconds... ({e})")
            time.sleep(delay * (attempt + 1))

    print("Failed to connect to database after multiple attempts. Exiting.")
    exit(1)

def is_market_hours(timestamp):
    """
    Check if the given timestamp is within market hours (9:30 AM - 4:00 PM ET, weekdays).
    """
    # Convert UTC timestamp to NY timezone
    if isinstance(timestamp, str):
        dt = pd.to_datetime(timestamp, utc=True)
    else:
        dt = timestamp
    
    current_time_ny = dt.astimezone(ny_timezone)
    market_open_time = current_time_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = current_time_ny.replace(hour=16, minute=0, second=0, microsecond=0)

    # Skip weekends
    if current_time_ny.weekday() >= 5:
        return False
    
    return market_open_time <= current_time_ny < market_close_time

# ====== Kafka Operations ======
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

# Initialize Kafka producer
producer = connect_kafka()

# Fetch tickers
tickers = fetch_tickers_from_db()
if not tickers:
    print("No tickers available from DB. Exiting.")
    exit(1)

# ====== Alpaca Stream Handler ======
async def handle_trade_data(data):
    """
    Callback for live trade data from Alpaca. 
    Filters data to only send during market hours.
    """
    timestamp = pd.to_datetime(data.timestamp, utc=True)
    
    # Only send data if it's during market hours
    if not is_market_hours(timestamp):
        return
    
    row = {
        "ticker": data.symbol,
        "timestamp": timestamp.isoformat(),
        "price": data.price,
        "size": data.size,
        "exchange": data.exchange
    }
    print(f"Kafka (Real): {row}")
    producer.send(KAFKA_TOPIC, value=row)
    producer.flush()

# ====== Main Execution ======
async def main():
    """Main function that starts the Alpaca stream."""
    stream = StockDataStream(API_KEY, API_SECRET)
    
    # Subscribe to trades for all tickers
    for ticker in tickers:
        stream.subscribe_trades(handle_trade_data, ticker)
    
    print("Real data producer started. Listening for market hours data...")
    
    # Run the stream indefinitely
    await stream.run()

if __name__ == "__main__":
    asyncio.run(main())