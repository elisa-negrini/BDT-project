# from alpaca.data.live import StockDataStream
# from datetime import datetime, timedelta
# from kafka import KafkaProducer
# import time
# import pytz
# import pandas as pd
# import nest_asyncio
# import asyncio
# import json

# # Patch per ambienti embedded (notebook, PyCharm)
# nest_asyncio.apply()

# # Alpaca API config
# API_KEY = "PKWTO08TY1J3RJ4MHWHK"
# API_SECRET = "bNufl6Fcq7OwlxLjtAtk7z5SAGPcKQ5yqb7oH1as"

# # Kafka config
# KAFKA_BROKER = 'kafka:9092'
# KAFKA_TOPIC = 'stock_trades'

# def connect_kafka():
#     while True:
#         try:
#             producer = KafkaProducer(
#                 bootstrap_servers=KAFKA_BROKER,
#                 value_serializer=lambda v: json.dumps(v).encode('utf-8')
#             )
#             print("✅ Connessione a Kafka riuscita.")
#             return producer
#         except Exception as e:
#             print(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
#             time.sleep(5)

# producer = connect_kafka()

# # Lista dei 30 principali ticker S&P 500
# top_30_tickers = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# # Fuso orario italiano
# italy_timezone = pytz.timezone("Europe/Rome")

# # Stream Alpaca
# stream = StockDataStream(API_KEY, API_SECRET)

# # Callback per ogni trade ricevuto
# async def handle_trade_data(data):
#     row = {
#         "ticker": data.symbol,
#         "timestamp": pd.to_datetime(data.timestamp, utc=True).isoformat(),
#         "price": data.price,
#         "size": data.size,
#         "exchange": data.exchange
#     }

#     # Stampa e invia su Kafka
#     print("📤 Kafka:", row)
#     producer.send(KAFKA_TOPIC, value=row)
#     producer.flush()

# # Funzione principale
# async def main(duration_seconds=60):
#     for symbol in top_30_tickers:
#         stream.subscribe_trades(handle_trade_data, symbol)

#     stop_time = datetime.now(italy_timezone) + timedelta(seconds=duration_seconds)
#     stream_task = asyncio.create_task(stream.run())

#     while datetime.now(italy_timezone) < stop_time:
#         await asyncio.sleep(1)

#     await stream.stop()
#     print("✅ Streaming terminato.")

# # Avvio script
# if __name__ == "__main__":
#     asyncio.run(main(duration_seconds=60))




























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

# Patch for embedded environments (notebooks, PyCharm)
nest_asyncio.apply()

# Alpaca API config
API_KEY = "PKWTO08TY1J3RJ4MHWHK"
API_SECRET = "bNufl6Fcq7OwlxLjtAtk7z5SAGPcKQ5yqb7oH1as"

# Kafka config
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'stock_trades'

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

# List of 30 main tickers of S&P 500
top_30_tickers = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

# New York timezone for market hours
ny_timezone = pytz.timezone("America/New_York")

# Initialize the last price for each ticker for random data generation
last_prices = {ticker: random.uniform(200, 300) for ticker in top_30_tickers}

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

    for ticker in top_30_tickers:
        # Calculate the new price
        price_change = random.uniform(-1, 1)
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
    for symbol in top_30_tickers:
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