from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField, BooleanType
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import torch
import json
import re

# === Lista di 100 parole chiave finanza ===
FINANCE_KEYWORDS = [
    "stock", "stocks", "market", "finance", "invest", "investment", "money", "trading",
    "nasdaq", "nyse", "earnings", "profits", "loss", "dividend", "portfolio", "equity",
    "bond", "inflation", "deflation", "capital", "index", "etf", "mutual fund", "analyst",
    "bull", "bear", "asset", "liability", "valuation", "ipo", "margin", "short", "long",
    "call", "put", "option", "derivative", "buy", "sell", "share", "yield", "interest rate",
    "revenue", "debt", "economy", "financial", "growth", "forex", "crypto", "bitcoin",
    "dividends", "futures", "hedge", "tax", "return", "liquidity", "volatility", "leverage",
    "securities", "speculation", "regulation", "currency", "exchange", "blue chip", "valuation",
    "real estate", "retirement", "pension", "401k", "income", "capital gains", "dow", "s&p",
    "nasdaq 100", "earnings report", "quarterly", "guidance", "broker", "buyback", "ceo",
    "treasury", "yield curve", "fed", "interest", "devaluation", "recession", "gdp", "macroeconomics",
    "microeconomics", "finance news", "stonks", "robinhood", "option chain", "day trade", "swing trade"
]

# === Spark Session ===
spark = SparkSession.builder \
    .appName("SentimentReddit") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# === Kafka Config ===
SOURCE_TOPIC = "reddit"
TARGET_TOPIC = "reddit_sentiment"
KAFKA_BROKER = "kafka:9092"

# === Company Name to Ticker Map ===
COMPANY_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
    "meta": "META", "facebook": "META", "berkshire": "BRK.B", "tesla": "TSLA", "unitedhealth": "UNH",
    "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
    "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
    "chevron": "CVX", "merck": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
    "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
    "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO"
}

# === Lazy-loaded FinBERT ===
tokenizer = None
model = None

def compute_sentiment_score(text):
    try:
        global tokenizer, model
        if tokenizer is None or model is None:
            tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
        with torch.no_grad():
            logits = model(**inputs).logits
        probs = softmax(logits.numpy()[0])
        sentiment_score = float(probs[0]) - float(probs[2])
        return str(round(sentiment_score, 4))
    except Exception:
        return "0.0"

def extract_tickers(text):
    tickers = set()
    matches = re.findall(r"\$([A-Z]{1,5})", text.upper())
    for sym in matches:
        if sym in COMPANY_TICKER_MAP.values():
            tickers.add(sym)
    lowered = text.lower()
    for name, ticker in COMPANY_TICKER_MAP.items():
        if name in lowered:
            tickers.add(ticker)
    return json.dumps(list(tickers) if tickers else ["GENERAL"])

def to_kafka_payload(tickers_json, sentiment_score, timestamp):
    return json.dumps({
        "timestamp": timestamp,
        "social": "reddit",
        "ticker": json.loads(tickers_json),
        "sentiment_score": float(sentiment_score)
    })


def contains_finance_keywords(text):
    if text is None:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in FINANCE_KEYWORDS)

finance_filter_udf = udf(contains_finance_keywords, BooleanType())


# === UDF Registration ===
extract_tickers_udf = udf(extract_tickers, StringType())
get_sentiment_udf = udf(compute_sentiment_score, StringType())
to_kafka_row_udf = udf(to_kafka_payload, StringType())

# === Kafka Schema ===
schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user", StringType(), True)
])

# === Read from Kafka ===
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# === Process Stream ===
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .filter(finance_filter_udf(col("text"))) \
    .withColumn("tickers", extract_tickers_udf(col("text"))) \
    .withColumn("sentiment_score", get_sentiment_udf(col("text"))) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("value", to_kafka_row_udf(
        col("tickers"), col("sentiment_score"), col("timestamp").cast("string"))
    )

# === Write to Kafka ===
query_kafka = df_parsed.selectExpr("CAST(value AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TARGET_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoints/reddit_kafka") \
    .start()

# === Also print to console for debugging ===
query_console = df_parsed.select("value") \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()

# === Wait for termination ===
query_kafka.awaitTermination()
query_console.awaitTermination()
