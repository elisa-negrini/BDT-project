from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import torch
import json
import re

# 1. Spark Session
spark = SparkSession.builder \
    .appName("SentimentBluesky") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Kafka config
SOURCE_TOPIC = "bluesky"
TARGET_TOPIC = "bluesky_sentiment"
KAFKA_BROKER = "kafka:9092"

# 3. Mapping nomi aziendali → ticker
COMPANY_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
    "meta": "META", "facebook": "META", "berkshire": "BRK.B", "tesla": "TSLA", "unitedhealth": "UNH",
    "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
    "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
    "chevron": "CVX", "merck": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
    "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
    "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO", "ibm" : "IBM"
}

# 4. Lazy-load modello FinBERT
tokenizer = None
model = None

def get_finbert_sentiment(text):
    try:
        global tokenizer, model
        if tokenizer is None or model is None:
            tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
        with torch.no_grad():
            logits = model(**inputs).logits
        probs = softmax(logits.numpy()[0])
        sentiment_score = float(probs[0] - probs[2])  # positive - negative
        return str(sentiment_score)
    except Exception as e:
        return "0.0"

# 5. Estrazione ticker da testo
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

# 6. Creazione JSON da scrivere su Kafka
def to_kafka_payload(tickers_json, sentiment_score_str, timestamp):
    return json.dumps({
        "timestamp": timestamp,
        "social": "bluesky",
        "ticker": json.loads(tickers_json),
        "sentiment": float(sentiment_score_str)
    })

# 7. UDF registration
extract_tickers_udf = udf(extract_tickers, StringType())
get_sentiment_udf = udf(get_finbert_sentiment, StringType())
to_kafka_row_udf = udf(to_kafka_payload, StringType())

# 8. Schema dati in input da Kafka
schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user", StringType(), True),
    StructField("created_at", StringType(), True) 
])

# 9. Lettura stream da Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# 10. Parsing + sentiment + ticker + timestamp
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn("tickers", extract_tickers_udf(col("text"))) \
    .withColumn("sentiment", get_sentiment_udf(col("text"))) \
    .withColumn("timestamp", col("created_at")) \
    .withColumn("value", to_kafka_row_udf(
        col("tickers"), col("sentiment"), col("timestamp"))
    )

# 11. Scrittura su Kafka (risultato finale)
query_kafka = df_parsed.selectExpr("CAST(value AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TARGET_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoints/kafka") \
    .start()

# 12. Stampa su console per debugging
query_console = df_parsed.select("value") \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .start()

# 13. Attendi fine streaming
query_kafka.awaitTermination()
query_console.awaitTermination()
