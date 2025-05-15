import requests
import time
import json
from datetime import date, timedelta
from kafka import KafkaProducer
import pandas as pd
import os

# ✅ Avvio script
print("🚀 Avvio producer notizie Finnhub")

# === CONFIG ===
API_KEY = "d056jb9r01qoigrsmf5gd056jb9r01qoigrsmf60"
KAFKA_BROKER = os.environ.get("KAFKA_SERVER", "kafka:9092")
KAFKA_TOPIC = 'finnhub'

tickers = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

# === STATO ===
sent_ids = set()

# === CONNESSIONE KAFKA ===
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Connessione a Kafka riuscita.")
            return producer
        except Exception as e:
            print(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
            time.sleep(5)

producer = connect_kafka()

# === FUNZIONE PRINCIPALE ===
def fetch_and_send_news():
    while True:
        today = date.today()
        from_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        to_date = today.strftime("%Y-%m-%d")
        print(f"\n🔁 Inizio ciclo - Notizie da {from_date} a {to_date}")

        for i, symbol in enumerate(tickers):
            url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={to_date}&token={API_KEY}"
            try:
                response = requests.get(url)

                if response.status_code != 200:
                    print(f"❌ Errore API {symbol}: {response.text[:200]}")
                    continue

                articles = response.json()
                print(f"[{i+1:02d}] {symbol}: {len(articles)} notizie trovate")

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

                    print(f"📤 Inviata notizia: {news_data['headline'][:60]}...")
                    producer.send(KAFKA_TOPIC, value=news_data)

            except Exception as e:
                print(f"❌ Errore durante richiesta per {symbol}: {e}")

            time.sleep(2)  # evita rate limit

        print("⏳ Pausa 60 secondi...\n")
        time.sleep(60)

# === AVVIO LOOP ===
fetch_and_send_news()
