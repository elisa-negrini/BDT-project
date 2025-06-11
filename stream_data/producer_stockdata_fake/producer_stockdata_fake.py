from datetime import datetime
from kafka import KafkaProducer
import time
import pytz
import asyncio
import json
import random
import os
import psycopg2

# ====== Configuration ======
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
    """Retrieves active stock tickers and their avg_simulated_price from the PostgreSQL database."""
    max_retries = 10
    delay = 5

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute("SELECT ticker, avg_simulated_price FROM companies_info WHERE is_active = TRUE AND avg_simulated_price IS NOT NULL;")
            result = cursor.fetchall()
            cursor.close()
            conn.close()

            if not result:
                print("No tickers with avg_simulated_price found.")
                return {}

            ticker_price_map = {row[0]: float(row[1]) for row in result if row[0] and row[1]}
            print(f"Loaded {len(ticker_price_map)} tickers with avg_simulated_price from DB.")
            return ticker_price_map

        except Exception as e:
            print(f"Database unavailable, retrying in {delay * (attempt + 1)} seconds... ({e})")
            time.sleep(delay * (attempt + 1))

    print("Failed to connect to database after multiple attempts. Exiting.")
    exit(1)

def is_outside_market_hours():
    """
    Check if current time is outside market hours (before 9:30 AM or after 4:00 PM ET, or weekends).
    """
    current_time_ny = datetime.now(ny_timezone)
    market_open_time = current_time_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = current_time_ny.replace(hour=16, minute=0, second=0, microsecond=0)

    # Always send on weekends
    if current_time_ny.weekday() >= 5:
        return True
    
    # Send before market open or after market close
    return current_time_ny < market_open_time or current_time_ny >= market_close_time

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

# Fetch tickers and initialize prices
ticker_price_map = fetch_tickers_from_db()
if not ticker_price_map:
    print("No tickers available from DB. Exiting.")
    exit(1)

tickers = list(ticker_price_map.keys())
last_prices = {ticker: ticker_price_map[ticker] for ticker in tickers}

# ====== Fake Data Generation ======
async def generate_fake_trade_data():
    """
    Generates simulated stock trade data for all tickers.
    Only sends data when outside market hours.
    """
    global last_prices
    
    # Only generate data if outside market hours
    if not is_outside_market_hours():
        return
    
    current_time_utc = datetime.now(pytz.utc).isoformat()

    for ticker in tickers:
        price_change = random.uniform(-0.05, 0.05)
        new_price = last_prices[ticker] + price_change
        if new_price < 10:  # Ensure price doesn't drop too low
            new_price = 10 + random.uniform(0, 0.05)
        last_prices[ticker] = new_price
        size = float(random.randint(1, 500))

        row = {
            "ticker": ticker,
            "timestamp": current_time_utc,
            "price": round(new_price, 2),
            "size": size,
            "exchange": "RANDOM"
        }
        print(f"Kafka (Fake): {row}")
        producer.send(KAFKA_TOPIC, value=row)
    
    producer.flush()

# ====== Main Execution ======
async def main():
    """Main function that generates fake data outside market hours."""
    print("Fake data producer started. Generating data outside market hours...")
    
    while True:
        await generate_fake_trade_data()
        await asyncio.sleep(1)  # Wait 1 second between data cycles

if __name__ == "__main__":
    asyncio.run(main())