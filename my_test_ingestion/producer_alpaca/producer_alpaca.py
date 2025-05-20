from alpaca.data.live import StockDataStream
from datetime import datetime, timedelta
from kafka import KafkaProducer
import time
import pytz
import pandas as pd
import nest_asyncio
import asyncio
import json

# Patch per ambienti embedded (notebook, PyCharm)
nest_asyncio.apply()

# Alpaca API config
API_KEY = "PKHIS2YU2ZX3NZBMWR7A"
API_SECRET = "YcgA9fe5J3QpFTptbnZtaUMwtb44J1P6KSAYya34"

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
            print("✅ Connessione a Kafka riuscita.")
            return producer
        except Exception as e:
            print(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
            time.sleep(5)

producer = connect_kafka()

# Lista dei 30 principali ticker S&P 500
top_30_tickers = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

# Fuso orario italiano
italy_timezone = pytz.timezone("Europe/Rome")

# Stream Alpaca
stream = StockDataStream(API_KEY, API_SECRET)

# Callback per ogni trade ricevuto
async def handle_trade_data(data):
    row = {
        "ticker": data.symbol,
        "timestamp": pd.to_datetime(data.timestamp, utc=True).isoformat(),
        "price": data.price,
        "size": data.size,
        "exchange": data.exchange
    }

    # Stampa e invia su Kafka
    print("📤 Kafka:", row)
    producer.send(KAFKA_TOPIC, value=row)
    producer.flush()

# Funzione principale
async def main(duration_seconds=60):
    for symbol in top_30_tickers:
        stream.subscribe_trades(handle_trade_data, symbol)

    stop_time = datetime.now(italy_timezone) + timedelta(seconds=duration_seconds)
    stream_task = asyncio.create_task(stream.run())

    while datetime.now(italy_timezone) < stop_time:
        await asyncio.sleep(1)

    await stream.stop()
    print("✅ Streaming terminato.")

# Avvio script
if __name__ == "__main__":
    asyncio.run(main(duration_seconds=60))
