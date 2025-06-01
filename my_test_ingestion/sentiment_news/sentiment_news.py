# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, col, from_json, current_timestamp, concat_ws
# from pyspark.sql.types import StringType, StructType, StructField
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# from scipy.special import softmax
# import torch
# import json

# # === Spark Session ===
# spark = SparkSession.builder \
#     .appName("SentimentNews") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("WARN")

# # === Kafka Config ===
# SOURCE_TOPIC = "finnhub"
# TARGET_TOPIC = "news_sentiment"
# KAFKA_BROKER = "kafka:9092"

# # === Lazy-loaded FinBERT model ===
# tokenizer = None
# model = None

# def compute_sentiment_score(text):
#     try:
#         global tokenizer, model
#         if tokenizer is None or model is None:
#             tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
#             model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

#         inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
#         with torch.no_grad():
#             logits = model(**inputs).logits
#         probs = softmax(logits.numpy()[0])

#         # Apply same logic as Twitter sentiment
#         sentiment_score = float(probs[0]) - float(probs[2])
#         return str(round(sentiment_score, 4))
#     except Exception:
#         return "0.0"

# # === Create Kafka JSON payload ===
# def to_kafka_payload(symbol_requested, sentiment_score, timestamp):
#     return json.dumps({
#         "timestamp": timestamp,
#         "social": "news",
#         "ticker": [symbol_requested],
#         "sentiment_score": float(sentiment_score)
#     })

# # === UDF Registration ===
# get_sentiment_udf = udf(compute_sentiment_score, StringType())
# to_kafka_row_udf = udf(to_kafka_payload, StringType())

# # === Kafka schema ===
# schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("symbol_requested", StringType(), True),
#     StructField("related", StringType(), True),
#     StructField("date", StringType(), True),
#     StructField("headline", StringType(), True),
#     StructField("summary", StringType(), True),
#     StructField("source", StringType(), True),
#     StructField("url", StringType(), True),
#     StructField("image", StringType(), True)
# ])

# # === Read from Kafka ===
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", SOURCE_TOPIC) \
#     .option("startingOffsets", "latest") \
#     .load()

# # === Transform stream ===
# df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json("json_str", schema).alias("data")) \
#     .select("data.*") \
#     .withColumn("text", concat_ws(" ", col("headline"), col("summary"))) \
#     .withColumn("sentiment_score", get_sentiment_udf(col("text"))) \
#     .withColumn("timestamp", col("date")) \
#     .withColumn("value", to_kafka_row_udf(
#         col("symbol_requested"), col("sentiment_score"), col("timestamp"))
#     )

# # === Write to Kafka ===
# query_kafka = df_parsed.selectExpr("CAST(value AS STRING) as value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("topic", TARGET_TOPIC) \
#     .option("checkpointLocation", "/tmp/checkpoints/news_kafka") \
#     .start()

# # === Optional: print to console ===
# query_console = df_parsed.select("value") \
#     .writeStream \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# query_kafka.awaitTermination()
# query_console.awaitTermination()



# import json
# import re
# import os
# import sys
# import numpy as np
# from datetime import datetime, timezone
# from scipy.special import softmax
# import onnxruntime as ort

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.common.serialization import SimpleStringSchema
# from transformers import AutoTokenizer

# # Kafka config
# SOURCE_TOPIC = "finnhub"
# TARGET_TOPIC = "news_sentiment"
# KAFKA_BROKER = "kafka:9092"

# COMPANY_TICKER_MAP = {
#     "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
#     "meta": "META", "facebook": "META", "oracle": "ORCL", "tesla": "TSLA", "unitedhealth": "UNH",
#     "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
#     "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
#     "chevron": "CVX", "mrk": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
#     "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
#     "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO", "ibm" : "IBM"
# }

# # Global model and tokenizer (lazy loaded in a more robust way)
# finbert_tokenizer = None
# finbert_session = None

# # def load_finbert_model():
# #     """
# #     Loads the FinBERT tokenizer and ONNX session.
# #     This function will be called once per worker/process.
# #     """
# #     global finbert_tokenizer, finbert_session
# #     if finbert_tokenizer is None:
# #         try:
# #             # Paths are relative to the working_dir /app
# #             tokenizer_path = "/app/model/tokenizer"
# #             if not os.path.isdir(tokenizer_path):
# #                 sys.stderr.write(f"ERROR: Tokenizer directory not found at {tokenizer_path}\n")
# #                 raise FileNotFoundError(f"Tokenizer directory not found: {tokenizer_path}")
# #             finbert_tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
# #             sys.stderr.write(f"INFO: FinBERT tokenizer loaded from {tokenizer_path}\n")
# #         except Exception as e:
# #             sys.stderr.write(f"CRITICAL ERROR: Failed to load FinBERT tokenizer: {e}\n")
# #             raise

# #     if finbert_session is None:
# #         try:
# #             # Path for the ONNX model file
# #             model_path = "/app/model/model.onnx"
# #             if not os.path.exists(model_path):
# #                 sys.stderr.write(f"ERROR: ONNX model file not found at {model_path}\n")
# #                 raise FileNotFoundError(f"ONNX model file not found: {model_path}")
# #             finbert_session = ort.InferenceSession(model_path)
# #             sys.stderr.write(f"INFO: FinBERT ONNX session loaded from {model_path}\n")
# #         except Exception as e:
# #             sys.stderr.write(f"CRITICAL ERROR: Failed to load FinBERT ONNX session: {e}\n")
# #             raise


# def load_finbert_model():
#     """
#     Loads the FinBERT tokenizer and ONNX session.
#     """
#     global finbert_tokenizer, finbert_session
    
#     # Prende il percorso base del modello dalla variabile d'ambiente, o usa /model come default
#     base_model_dir = os.environ.get("FINBERT_MODEL_BASE_PATH", "/model")

#     if finbert_tokenizer is None:
#         try:
#             tokenizer_path = os.path.join(base_model_dir, "tokenizer")
#             if not os.path.isdir(tokenizer_path):
#                 sys.stderr.write(f"ERROR: Tokenizer directory not found at {tokenizer_path}\n")
#                 raise FileNotFoundError(f"Tokenizer directory not found: {tokenizer_path}")
#             finbert_tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
#             sys.stderr.write(f"INFO: FinBERT tokenizer loaded from {tokenizer_path}\n")
#         except Exception as e:
#             sys.stderr.write(f"CRITICAL ERROR: Failed to load FinBERT tokenizer: {e}\n")
#             raise

#     if finbert_session is None:
#         try:
#             model_path = os.path.join(base_model_dir, "model.onnx")
#             if not os.path.exists(model_path):
#                 sys.stderr.write(f"ERROR: ONNX model file not found at {model_path}\n")
#                 raise FileNotFoundError(f"ONNX model file not found: {model_path}")
#             finbert_session = ort.InferenceSession(model_path)
#             sys.stderr.write(f"INFO: FinBERT ONNX session loaded from {model_path}\n")
#         except Exception as e:
#             sys.stderr.write(f"CRITICAL ERROR: Failed to load FinBERT ONNX session: {e}\n")
#             raise



# def extract_tickers(text):
#     tickers = set()
#     matches = re.findall(r"\$([A-Z]{1,5})", text.upper())
#     for sym in matches:
#         if sym in COMPANY_TICKER_MAP.values():
#             tickers.add(sym)
#     lowered = text.lower()
#     for name, ticker in COMPANY_TICKER_MAP.items():
#         if name in lowered:
#             tickers.add(ticker)
#     return list(tickers) if tickers else ["GENERAL"]


# def compute_sentiment(text):
#     # Ensure model and tokenizer are loaded before use
#     load_finbert_model()

#     try:
#         if not text or text.strip() == "":
#             return 0.0
#         tokens = finbert_tokenizer(
#             text,
#             return_tensors="np",
#             truncation=True,
#             padding="max_length",
#             max_length=128
#         )
#         ort_inputs = {k: v for k, v in tokens.items()}
#         logits = finbert_session.run(None, ort_inputs)[0]
#         probs = softmax(logits[0])
#         return round(float(probs[0] - probs[2]), 4)
#     except Exception as e:
#         sys.stderr.write(f"Error in compute_sentiment for text (first 50 chars): '{text[:50]}...': {e}\n")
#         return 0.0


# def process_message(msg):
#     try:
#         data = json.loads(msg)
#         text = data.get("text", "")
#         created_at = data.get("created_at", "")

#         timestamp_str_parsed = ""
#         try:
#             match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z)?", created_at)
#             if match:
#                 base_time = match.group(1)
#                 fractional_seconds = match.group(2)
#                 timezone_z = match.group(3)

#                 if fractional_seconds:
#                     fractional_seconds = fractional_seconds[:7]
#                 else:
#                     fractional_seconds = ""

#                 timestamp_to_parse = f"{base_time}{fractional_seconds}{timezone_z if timezone_z else ''}"
                
#                 timestamp_obj = datetime.fromisoformat(timestamp_to_parse.replace("Z", "+00:00"))
                
#                 timestamp_obj_utc = timestamp_obj.astimezone(timezone.utc)
#                 timestamp_str_parsed = timestamp_obj_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
#             else:
#                 raise ValueError("Timestamp format not recognized by regex.")
#         except ValueError as ve:
#             sys.stderr.write(f"WARN: Could not parse timestamp '{created_at}' due to format error: {ve}. Using current UTC time.\n")
#             timestamp_str_parsed = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
#         except Exception as e:
#             sys.stderr.write(f"ERROR: Unexpected error during timestamp parsing for '{created_at}': {e}. Using current UTC time.\n")
#             timestamp_str_parsed = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'

#         tickers = extract_tickers(text)
#         score = compute_sentiment(text)

#         out = {
#             "timestamp": timestamp_str_parsed,
#             "social": "news",
#             "ticker": tickers,
#             "sentiment_score": score
#         }

#         return json.dumps(out)
#     except Exception as e:
#         sys.stderr.write(f"Failed to process message: {e}\n")
#         return json.dumps({
#             "timestamp": datetime.utcnow().isoformat(timespec='milliseconds') + 'Z',
#             "social": "news",
#             "ticker": ["ERROR"],
#             "sentiment_score": 0.0,
#             "error": str(e)
#         })

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     # Kafka source
#     kafka_consumer = FlinkKafkaConsumer(
#         topics=SOURCE_TOPIC,
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             "bootstrap.servers": KAFKA_BROKER,
#             "group.id": "sentiment-flink-group",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     # Kafka sink (your primary output)
#     kafka_producer = FlinkKafkaProducer(
#         topic=TARGET_TOPIC,
#         serialization_schema=SimpleStringSchema(),
#         producer_config={"bootstrap.servers": KAFKA_BROKER}
#     )

#     # Stream processing pipeline
#     processed_stream = env.add_source(kafka_consumer) \
#         .map(lambda msg: process_message(msg), output_type=Types.STRING())
    
#     # Add Kafka sink
#     processed_stream.add_sink(kafka_producer)

#     # --- INIZIO NUOVA MODIFICA PER LA CONSOLE SINK ---
#     # Aggiungi una console sink per stampare i messaggi processati sul log del container
#     processed_stream.print()
#     # --- FINE NUOVA MODIFICA PER LA CONSOLE SINK ---

#     # Execute the Flink job
#     print("Starting Flink job: Sentiment Analysis with Flink", file=sys.stderr)
#     env.execute("Sentiment Analysis with Flink")
#     print("Flink job finished.", file=sys.stderr)

# if __name__ == "__main__":
#     main()


import json
import re
import os
import sys
import numpy as np
from datetime import datetime, timezone
from scipy.special import softmax
import onnxruntime as ort

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from transformers import AutoTokenizer

# Kafka config
SOURCE_TOPIC = "finnhub" # <--- Cambiato per le news
TARGET_TOPIC = "news_sentiment" # <--- Cambiato per le news
KAFKA_BROKER = "kafka:9092"

COMPANY_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
    "meta": "META", "facebook": "META", "oracle": "ORCL", "tesla": "TSLA", "unitedhealth": "UNH",
    "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
    "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
    "chevron": "CVX", "mrk": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
    "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
    "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO", "ibm" : "IBM"
}

# Global model and tokenizer (lazy loaded)
finbert_tokenizer = None
finbert_session = None

def load_finbert_model():
    """
    Loads the FinBERT tokenizer and ONNX session.
    """
    global finbert_tokenizer, finbert_session
    
    # Prende il percorso base del modello dalla variabile d'ambiente, o usa /model come default
    base_model_dir = os.environ.get("FINBERT_MODEL_BASE_PATH", "/model")

    if finbert_tokenizer is None:
        try:
            tokenizer_path = os.path.join(base_model_dir, "tokenizer")
            if not os.path.isdir(tokenizer_path):
                sys.stderr.write(f"ERROR: Tokenizer directory not found at {tokenizer_path}\n")
                raise FileNotFoundError(f"Tokenizer directory not found: {tokenizer_path}")
            finbert_tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
            sys.stderr.write(f"INFO: FinBERT tokenizer loaded from {tokenizer_path}\n")
        except Exception as e:
            sys.stderr.write(f"CRITICAL ERROR: Failed to load FinBERT tokenizer: {e}\n")
            raise

    if finbert_session is None:
        try:
            model_path = os.path.join(base_model_dir, "model.onnx")
            if not os.path.exists(model_path):
                sys.stderr.write(f"ERROR: ONNX model file not found at {model_path}\n")
                raise FileNotFoundError(f"ONNX model file not found: {model_path}")
            finbert_session = ort.InferenceSession(model_path)
            sys.stderr.write(f"INFO: FinBERT ONNX session loaded from {model_path}\n")
        except Exception as e:
            sys.stderr.write(f"CRITICAL ERROR: Failed to load FinBERT ONNX session: {e}\n")
            raise

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
    return list(tickers) if tickers else ["GENERAL"]


def compute_sentiment(text):
    # Ensure model and tokenizer are loaded before use
    load_finbert_model()

    try:
        if not text or text.strip() == "":
            return 0.0
        tokens = finbert_tokenizer(
            text,
            return_tensors="np",
            truncation=True,
            padding="max_length",
            max_length=128
        )
        ort_inputs = {k: v for k, v in tokens.items()}
        logits = finbert_session.run(None, ort_inputs)[0]
        probs = softmax(logits[0])
        return round(float(probs[0] - probs[2]), 4)
    except Exception as e:
        sys.stderr.write(f"Error in compute_sentiment for text (first 50 chars): '{text[:50]}...': {e}\n")
        return 0.0


def process_message(msg):
    try:
        data = json.loads(msg)
        
        # --- Modifiche per i campi specifici delle news ---
        headline = data.get("headline", "")
        summary = data.get("summary", "")
        symbol_requested = data.get("symbol_requested", "UNKNOWN") # Ticker dalla news
        created_at_input = data.get("date", "") # Il campo timestamp è 'date'
        
        # Concatena headline e summary per il testo di sentiment
        text_for_sentiment = f"{headline} {summary}".strip()

        # --- Gestione Timestamp (semplificata come in PySpark) ---
        # Se 'date' è già nel formato corretto per il consumer, passalo direttamente.
        # Altrimenti, applica la logica di parsing e formattazione come per Bluesky.
        # Per ora, assumiamo che 'date' sia già buono.
        timestamp_str_output = created_at_input 
        
        # Se in futuro il consumer si lamenta ancora, puoi reintrodurre la logica di parsing robusta:
        # try:
        #     match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z)?", created_at_input)
        #     if match:
        #         base_time = match.group(1)
        #         fractional_seconds = match.group(2)
        #         timezone_z = match.group(3)
        #         if fractional_seconds: fractional_seconds = fractional_seconds[:7]
        #         else: fractional_seconds = ""
        #         timestamp_to_parse = f"{base_time}{fractional_seconds}{timezone_z if timezone_z else ''}"
        #         timestamp_obj = datetime.fromisoformat(timestamp_to_parse.replace("Z", "+00:00"))
        #         timestamp_obj_utc = timestamp_obj.astimezone(timezone.utc)
        #         timestamp_str_output = timestamp_obj_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        #     else:
        #         raise ValueError("Timestamp format not recognized by regex.")
        # except ValueError as ve:
        #     sys.stderr.write(f"WARN: Could not parse timestamp '{created_at_input}' for news: {ve}. Using current UTC time.\n")
        #     timestamp_str_output = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
        # except Exception as e:
        #     sys.stderr.write(f"ERROR: Unexpected error during timestamp parsing for news '{created_at_input}': {e}. Using current UTC time.\n")
        #     timestamp_str_output = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'


        tickers = extract_tickers(text_for_sentiment)
        score = compute_sentiment(text_for_sentiment)

        out = {
            "timestamp": timestamp_str_output,
            "social": "news", # <--- Hardcodato a "news"
            "ticker": [symbol_requested], # <--- Il ticker è una lista con symbol_requested
            "sentiment_score": score
        }

        return json.dumps(out)
    except Exception as e:
        sys.stderr.write(f"Failed to process message for news: {e}\n")
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(timespec='milliseconds') + 'Z',
            "social": "news",
            "ticker": ["ERROR"],
            "sentiment_score": 0.0,
            "error": str(e)
        })

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka source
    kafka_consumer = FlinkKafkaConsumer(
        topics=SOURCE_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "news-sentiment-flink-group", # <--- Gruppo ID specifico
            "auto.offset.reset": "earliest"
        }
    )

    # Kafka sink
    kafka_producer = FlinkKafkaProducer(
        topic=TARGET_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BROKER}
    )

    # Stream processing pipeline
    processed_stream = env.add_source(kafka_consumer) \
        .map(lambda msg: process_message(msg), output_type=Types.STRING())
    
    # Add Kafka sink
    processed_stream.add_sink(kafka_producer)

    # Add console sink for debugging
    processed_stream.print()

    # Execute the Flink job
    print("Starting Flink job: News Sentiment Analysis with Flink", file=sys.stderr)
    env.execute("News Sentiment Analysis with Flink")
    print("Flink job finished.", file=sys.stderr)

if __name__ == "__main__":
    main()
