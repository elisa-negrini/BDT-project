from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import torch
import json
import re

# Spark Session
spark = SparkSession.builder \
    .appName("SentimentReddit") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# # MinIO config (S3 compatible)
# spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
# spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
# spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "admin123")
# spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Kafka config
SOURCE_TOPIC = "reddit"
TARGET_TOPIC = "reddit_sentiment"
KAFKA_BROKER = "kafka:9092"

# Ticker mapping
COMPANY_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
    "meta": "META", "facebook": "META", "berkshire": "BRK.B", "tesla": "TSLA", "unitedhealth": "UNH",
    "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
    "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
    "chevron": "CVX", "merck": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
    "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
    "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO"
}

# Lazy-loaded model
tokenizer = None
model = None

def get_finbert_sentiment(text):
    global tokenizer, model
    if tokenizer is None or model is None:
        tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = softmax(logits.numpy()[0])
    return json.dumps({
        "positive_prob": float(probs[0]),
        "neutral_prob": float(probs[1]),
        "negative_prob": float(probs[2])
    })

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

def to_kafka_payload(id, text, user, tickers_json, sentiment_json, timestamp):
    data = {
        "id": id,
        "text": text,
        "user": user,
        "ticker": json.loads(tickers_json),
        **json.loads(sentiment_json),
        "timestamp": timestamp
    }
    return json.dumps(data)

# UDF registration
extract_tickers_udf = udf(extract_tickers, StringType())
get_sentiment_udf = udf(get_finbert_sentiment, StringType())
to_kafka_row_udf = udf(to_kafka_payload, StringType())

# Kafka schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user", StringType(), True)
])

# Read Kafka stream
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Transform data
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn("tickers", extract_tickers_udf(col("text"))) \
    .withColumn("sentiment", get_sentiment_udf(col("text"))) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("value", to_kafka_row_udf(
        col("id"), col("text"), col("user"), col("tickers"), col("sentiment"), col("timestamp").cast("string"))
    )

# Stream to Kafka
query_kafka = df_parsed.selectExpr("CAST(value AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TARGET_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoints/kafka") \
    .start()

# # Stream to MinIO
# query_minio = df_parsed.select("value") \
#     .writeStream \
#     .format("json") \
#     .option("path", "s3a://bluesky-data/") \
#     .option("checkpointLocation", "/tmp/checkpoints/minio") \
#     .outputMode("append") \
#     .start()

# Wait for any stream to finish
query_kafka.awaitTermination()
