from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, current_timestamp, concat_ws
from pyspark.sql.types import StringType, StructType, StructField
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import torch
import json

# === Spark Session ===
spark = SparkSession.builder \
    .appName("SentimentNews") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# === Kafka Config ===
SOURCE_TOPIC = "finnhub"
TARGET_TOPIC = "news_sentiment"
KAFKA_BROKER = "kafka:9092"

# === Lazy-loaded FinBERT model ===
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

        # Apply same logic as Twitter sentiment
        sentiment_score = float(probs[0]) - float(probs[2])
        return str(round(sentiment_score, 4))
    except Exception:
        return "0.0"

# === Create Kafka JSON payload ===
def to_kafka_payload(symbol_requested, sentiment_score, timestamp):
    return json.dumps({
        "timestamp": timestamp,
        "social": "news",
        "ticker": [symbol_requested],
        "sentiment_score": float(sentiment_score)
    })

# === UDF Registration ===
get_sentiment_udf = udf(compute_sentiment_score, StringType())
to_kafka_row_udf = udf(to_kafka_payload, StringType())

# === Kafka schema ===
schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol_requested", StringType(), True),
    StructField("related", StringType(), True),
    StructField("date", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("source", StringType(), True),
    StructField("url", StringType(), True),
    StructField("image", StringType(), True)
])

# === Read from Kafka ===
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# === Transform stream ===
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn("text", concat_ws(" ", col("headline"), col("summary"))) \
    .withColumn("sentiment_score", get_sentiment_udf(col("text"))) \
    .withColumn("timestamp", col("date")) \
    .withColumn("value", to_kafka_row_udf(
        col("symbol_requested"), col("sentiment_score"), col("timestamp"))
    )

# === Write to Kafka ===
query_kafka = df_parsed.selectExpr("CAST(value AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TARGET_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoints/news_kafka") \
    .start()

# === Optional: print to console ===
query_console = df_parsed.select("value") \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

query_kafka.awaitTermination()
query_console.awaitTermination()
