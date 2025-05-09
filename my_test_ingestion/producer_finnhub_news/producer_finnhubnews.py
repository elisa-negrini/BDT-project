import requests
import pandas as pd
import time
import json
from datetime import date
from kafka import KafkaProducer

# Carica simboli S&P 500
top_30_tickers = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "GOOG",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]
tickers = top_30_tickers

# Chiave API
API_KEY = "d056jb9r01qoigrsmf5gd056jb9r01qoigrsmf60"

# Kafka config
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic = "finnhub_news"

# Set per tracciare notizie già inviate
sent_ids = set()


# Ciclo infinito
while True:
    today = date.today().strftime("%Y-%m-%d")
    FROM_DATE = TO_DATE = today
    print(f"\n🔁 Inizio ciclo per il {FROM_DATE}")

    for i, symbol in enumerate(tickers):
        url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={FROM_DATE}&to={TO_DATE}&token={API_KEY}"

        try:
            response = requests.get(url)
            data = response.json()
            print(f"[{i+1:03d}] {symbol}: {len(data)} notizie trovate")

            for news in data:
                news_id = news.get("id")
                if news_id is None or news_id in sent_ids:
                    continue

                sent_ids.add(news_id)

                item = {
                    "date": pd.to_datetime(news['datetime'], unit='s').isoformat(),
                    "symbol_requested": symbol,
                    "related": news.get("related", ""),
                    "headline": news.get("headline", ""),
                    "summary": news.get("summary", ""),
                    "source": news.get("source", ""),
                    "url": news.get("url", ""),
                    "image": news.get("image", ""),
                    "id": news_id
                }

                # Invia a Kafka
                producer.send(topic, value=item)
                print(f"📤 Inviata notizia: {item['headline'][:60]}...")

        except Exception as e:
            print(f"❌ Errore su {symbol}: {e}")

        time.sleep(2)  # evitare rate limit

    producer.flush()
    print("⏳ Pausa 1 minuto...\n")
    time.sleep(60)
