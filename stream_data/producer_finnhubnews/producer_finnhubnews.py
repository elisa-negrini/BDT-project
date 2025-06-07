import requests
import time
import json
from datetime import date, timedelta
from kafka import KafkaProducer
import pandas as pd
import os
import psycopg2

print("Starting Finnhub news producer...")

# === CONFIGURATION ===
API_KEY = os.getenv("FINNHUB_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = 'finnhub'

# === PostgreSQL CONFIG ===
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# === Load tickers from PostgreSQL ===
def load_tickers_from_db():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM companies_info WHERE is_active = TRUE;")
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows if row[0]]
    except Exception as e:
        print(f"Error fetching tickers from PostgreSQL: {e}")
        return []

tickers = load_tickers_from_db()
if not tickers:
    print("No tickers found in the database. Exiting.")
    exit(1)

# === Kafka producer connection ===
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Kafka producer connection established.")
            return producer
        except Exception as e:
            print(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

producer = connect_kafka()

# === News ID cache to prevent duplicates ===
sent_ids = set()

# === Main fetch and send function ===
def fetch_and_send_news():
    while True:
        today = date.today()
        from_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        to_date = today.strftime("%Y-%m-%d")
        print(f"\nFetching news from {from_date} to {to_date}")

        for i, symbol in enumerate(tickers):
            url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={to_date}&token={API_KEY}"
            try:
                response = requests.get(url)

                if response.status_code != 200:
                    print(f"API error for {symbol}: {response.text[:200]}")
                    continue

                articles = response.json()
                print(f"[{i+1:02d}] {symbol}: {len(articles)} articles found")

                for news in articles:
                    news_id = news.get("id")
                    if news_id is None or news_id in sent_ids:
                        continue

                    sent_ids.add(news_id)

                    news_data = {
                        "id": news_id,
                        "symbol_requested": symbol,
                        "related": news.get("related", ""),
                        "date": pd.to_datetime(news["datetime"], unit="s").isoformat(),
                        "headline": news.get("headline", ""),
                        "summary": news.get("summary", ""),
                        "source": news.get("source", ""),
                        "url": news.get("url", ""),
                        "image": news.get("image", "")
                    }

                    print(f"Sent news: {news_data['headline'][:60]}...")
                    producer.send(KAFKA_TOPIC, value=news_data)

            except Exception as e:
                print(f"Error during request for {symbol}: {e}")

            time.sleep(2)  # prevent API rate limits

        print("Waiting 60 seconds before next cycle...\n")
        time.sleep(60)

# === Start the loop ===
fetch_and_send_news()