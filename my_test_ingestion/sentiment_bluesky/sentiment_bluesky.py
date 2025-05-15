from kafka import KafkaConsumer, KafkaProducer
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import torch
import json
import time
import re

# Kafka Config
KAFKA_BROKER = 'kafka:9092'
SOURCE_TOPIC = 'bluesky'
TARGET_TOPIC = 'bluesky_sentiment'

# FinBERT Model
MODEL_NAME = "ProsusAI/finbert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

# Mappatura nome azienda -> ticker
COMPANY_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
    "meta": "META", "facebook": "META", "berkshire": "BRK.B", "tesla": "TSLA", "unitedhealth": "UNH",
    "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
    "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
    "chevron": "CVX", "merck": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
    "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
    "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO", "ibm" : "IBM"
}

def extract_tickers(text):
    tickers = set()

    # Match simboli $TICKER
    symbol_matches = re.findall(r"\$([A-Z]{1,5})", text.upper())
    for sym in symbol_matches:
        if sym in COMPANY_TICKER_MAP.values():
            tickers.add(sym)

    # Match nomi aziendali
    lowered_text = text.lower()
    for name, ticker in COMPANY_TICKER_MAP.items():
        if name in lowered_text:
            tickers.add(ticker)

    return list(tickers)

def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                SOURCE_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='json_saver_group'
            )
            print("✅ Connessione a Kafka riuscita (consumer).")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
            time.sleep(5)

def get_finbert_sentiment(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = softmax(logits.numpy()[0])
    return {
        "positive_prob": float(probs[0]),
        "neutral_prob": float(probs[1]),
        "negative_prob": float(probs[2])
    }

# Start Kafka consumer
consumer = connect_kafka_consumer()
print(f"📡 In ascolto sul topic '{SOURCE_TOPIC}'...")

for message in consumer:
    post = message.value
    text = post.get("text", "")

    if not text.strip():
        continue

    sentiment = get_finbert_sentiment(text)
    tickers = extract_tickers(text)

    # Se non trova nessun ticker, imposta 'GENERAL'
    post["ticker"] = tickers if tickers else ["GENERAL"]
    post.update(sentiment)

    print(f"🔍 Post: {post['id']} | Ticker: {post['ticker']} | Pos: {sentiment['positive_prob']:.2f}, Neg: {sentiment['negative_prob']:.2f}")
