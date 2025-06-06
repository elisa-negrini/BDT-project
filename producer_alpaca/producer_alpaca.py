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

# Patch for embedded environments (notebooks, PyCharm)
nest_asyncio.apply()

# Alpaca API config
API_KEY = os.getenv("API_KEY_ALPACA") 
API_SECRET = os.getenv("API_SECRET_ALPACA")

# Kafka config
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = 'stock_trades'

# Database connection details for fetching tickers - MUST be set as environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def fetch_tickers_from_db():
    max_retries = 10
    delay = 5

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT DISTINCT ticker FROM companies_info WHERE is_active = TRUE;")
            except psycopg2.ProgrammingError:
                print("Column 'is_active' not found. Falling back to all distinct tickers.")
                cursor.execute("SELECT DISTINCT ticker FROM companies_info;")

            result = cursor.fetchall()
            tickers = [row[0] for row in result if row[0]]
            cursor.close()
            conn.close()

            if not tickers:
                print("No tickers found in the database.")
            else:
                print(f"Loaded {len(tickers)} tickers from DB.")
            return tickers
        except Exception as e:
            print(f"Database not available, retrying in {delay * (attempt + 1)} seconds... ({e})")
            time.sleep(delay * (attempt + 1))

    print("Failed to connect to database after multiple attempts. Exiting.")
    exit(1)

top_tickers = fetch_tickers_from_db()
if not top_tickers:
    print("No tickers available from DB. Exiting.")
    exit(1)

def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka.")
            return producer
        except Exception as e:
            print(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

producer = connect_kafka()


# New York timezone for market hours
ny_timezone = pytz.timezone("America/New_York")

# Initialize the last price for each ticker for random data generation
last_prices = {ticker: random.uniform(200, 300) for ticker in top_tickers}

# Alpaca Stream
stream = StockDataStream(API_KEY, API_SECRET)
stream_running = False # Variable to track the state of the Alpaca stream

# Callback for each trade received from Alpaca
async def handle_trade_data(data):
    row = {
        "ticker": data.symbol,
        "timestamp": pd.to_datetime(data.timestamp, utc=True).isoformat(),
        "price": data.price,
        "size": data.size,
        "exchange": data.exchange
    }

    # Print and send to Kafka
    print(f"Kafka (Alpaca): {row}")
    producer.send(KAFKA_TOPIC, value=row)
    producer.flush()

# Function to generate random trade data
async def generate_random_trade_data():
    global last_prices
    current_time_utc = datetime.now(pytz.utc).isoformat()

    for ticker in top_tickers:
        # Calculate the new price
        price_change = random.uniform(-0.1, 0.1)
        new_price = last_prices[ticker] + price_change

        # Ensure the price never drops below 100
        if new_price < 100:
            new_price = 100 + random.uniform(0, 1) # Bring the price slightly above 100

        last_prices[ticker] = new_price

        # Generate integer size
        size = float(random.randint(1, 500))

        row = {
            "ticker": ticker,
            "timestamp": current_time_utc,
            "price": round(new_price, 2), # Round price to 2 decimal places
            "size": size,
            "exchange": "RANDOM" # Indicate that the data is randomly generated
        }

        print(f"Kafka (Random): {row}")
        producer.send(KAFKA_TOPIC, value=row)
    producer.flush()


# Function to check if the market is open (9:30 - 16:00 ET)
def is_market_open():
    current_time_ny = datetime.now(ny_timezone)
    market_open_time = current_time_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = current_time_ny.replace(hour=16, minute=0, second=0, microsecond=0)

    # Check if it's a weekday (0=Monday, 6=Sunday)
    if current_time_ny.weekday() >= 5: # Saturday or Sunday
        return False

    return market_open_time <= current_time_ny < market_close_time

# Main function
async def main():
    global stream_running

    # Subscribe to all tickers once
    for symbol in top_tickers:
        stream.subscribe_trades(handle_trade_data, symbol)

    print("Script started in infinite mode.")

    while True:
        if is_market_open():
            if not stream_running:
                print("Market open (New York). Starting Alpaca streaming...")
                asyncio.create_task(stream.run()) # Start the stream in the background
                stream_running = True
            await asyncio.sleep(1) # Let the Alpaca stream handle events
        else:
            if stream_running:
                print("Market closed (New York). Stopping Alpaca streaming and switching to random data generation...")
                await stream.stop() # Stop the Alpaca stream
                stream_running = False
            # Generate random data every second
            await generate_random_trade_data()
            await asyncio.sleep(1) # Wait one second before generating the next set of data

# Start the script
if __name__ == "__main__":
    asyncio.run(main())