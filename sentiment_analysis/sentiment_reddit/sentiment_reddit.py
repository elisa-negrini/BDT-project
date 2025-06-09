# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, col, from_json, current_timestamp,  from_unixtime, date_format
# from pyspark.sql.types import StringType, StructType, StructField, BooleanType
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# from scipy.special import softmax
# import torch
# import json
# import re


# # === Lista di 100 parole chiave finanza ===
# FINANCE_KEYWORDS = [
#     "stock", "stocks", "market", "finance", "invest", "investment", "money", "trading",
#     "nasdaq", "nyse", "earnings", "profits", "loss", "dividend", "portfolio", "equity",
#     "bond", "inflation", "deflation", "capital", "index", "etf", "mutual fund", "analyst",
#     "bull", "bear", "asset", "liability", "valuation", "ipo", "margin", "short", "long",
#     "call", "put", "option", "derivative", "buy", "sell", "share", "yield", "interest rate",
#     "revenue", "debt", "economy", "financial", "growth", "forex", "crypto", "bitcoin",
#     "dividends", "futures", "hedge", "tax", "return", "liquidity", "volatility", "leverage",
#     "securities", "speculation", "regulation", "currency", "exchange", "blue chip", "valuation",
#     "real estate", "retirement", "pension", "401k", "income", "capital gains", "dow", "s&p",
#     "nasdaq 100", "earnings report", "quarterly", "guidance", "broker", "buyback", "ceo",
#     "treasury", "yield curve", "fed", "interest", "devaluation", "recession", "gdp", "macroeconomics",
#     "microeconomics", "finance news", "stonks", "robinhood", "option chain", "day trade", "swing trade"
# ]

# # === Spark Session ===
# spark = SparkSession.builder \
#     .appName("SentimentReddit") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("WARN")

# # === Kafka Config ===
# SOURCE_TOPIC = "reddit"
# TARGET_TOPIC = "reddit_sentiment"
# KAFKA_BROKER = "kafka:9092"

# # === Company Name to Ticker Map ===
# COMPANY_TICKER_MAP = {
#     "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
#     "meta": "META", "facebook": "META", "berkshire": "BRK.B", "tesla": "TSLA", "unitedhealth": "UNH",
#     "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
#     "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
#     "chevron": "CVX", "merck": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
#     "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
#     "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO", "ibm" : "IBM"
# }

# # === Lazy-loaded FinBERT ===
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
#         sentiment_score = float(probs[0]) - float(probs[2])
#         return str(round(sentiment_score, 4))
#     except Exception:
#         return "0.0"

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
#     return json.dumps(list(tickers) if tickers else ["GENERAL"])

# def to_kafka_payload(tickers_json, sentiment_score, timestamp):
#     return json.dumps({
#         "timestamp": timestamp,
#         "social": "reddit",
#         "ticker": json.loads(tickers_json),
#         "sentiment_score": float(sentiment_score)
#     })


# def contains_finance_keywords(text):
#     if text is None:
#         return False
#     text_lower = text.lower()
#     return any(keyword in text_lower for keyword in FINANCE_KEYWORDS)




# # === UDF Registration ===
# extract_tickers_udf = udf(extract_tickers, StringType())
# get_sentiment_udf = udf(compute_sentiment_score, StringType())
# to_kafka_row_udf = udf(to_kafka_payload, StringType())
# finance_filter_udf = udf(contains_finance_keywords, BooleanType())

# # === Kafka Schema ===
# schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("text", StringType(), True),
#     StructField("user", StringType(), True),
#     StructField("created_utc", StringType(), True)  # timestamp originale Reddit
# ])

# # === Read from Kafka ===
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", SOURCE_TOPIC) \
#     .option("startingOffsets", "earlier") \
#     .load()

# # === Process Stream ===
# df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json("json_str", schema).alias("data")) \
#     .select("data.*") \
#     .withColumn("tickers", extract_tickers_udf(col("text"))) \
#     .withColumn("sentiment_score", get_sentiment_udf(col("text"))) \
#     .withColumn("timestamp", date_format(from_unixtime(col("created_utc").cast("double")), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
#     .withColumn("value", to_kafka_row_udf(
#         col("tickers"), col("sentiment_score"), col("timestamp"))
#     )
#     #.filter(finance_filter_udf(col("text")))

# # === Write to Kafka ===
# query_kafka = df_parsed.selectExpr("CAST(value AS STRING) as value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("topic", TARGET_TOPIC) \
#     .option("checkpointLocation", "/tmp/checkpoints/reddit_kafka") \
#     .start()

# # === Also print to console for debugging ===
# query_console = df_parsed.select("value") \
#     .writeStream \
#     .format("console") \
#     .option("truncate", False) \
#     .outputMode("append") \
#     .start()

# # === Wait for termination ===
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
# SOURCE_TOPIC = "reddit"
# TARGET_TOPIC = "reddit_sentiment"
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
#             "social": "reddit",
#             "ticker": tickers,
#             "sentiment_score": score
#         }

#         return json.dumps(out)
#     except Exception as e:
#         sys.stderr.write(f"Failed to process message: {e}\n")
#         return json.dumps({
#             "timestamp": datetime.utcnow().isoformat(timespec='milliseconds') + 'Z',
#             "social": "reddit",
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
SOURCE_TOPIC = "reddit" # <--- Cambiato per Reddit
TARGET_TOPIC = "reddit_sentiment" # <--- Cambiato per Reddit
KAFKA_BROKER = "kafka:9092"

COMPANY_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
    "meta": "META", "facebook": "META", "berkshire": "BRK.B", "tesla": "TSLA", "unitedhealth": "UNH",
    "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
    "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
    "chevron": "CVX", "merck": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
    "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
    "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO", "ibm" : "IBM"
}

# === Lista di parole chiave finanza ===
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


# Global model and tokenizer (lazy loaded)
finbert_tokenizer = None
finbert_session = None

def load_finbert_model():
    """
    Loads the FinBERT tokenizer and ONNX session.
    """
    global finbert_tokenizer, finbert_session
    
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
    # Assicurati che il modello e il tokenizer siano caricati
    load_finbert_model()

    try:
        if not text or text.strip() == "":
            return 0.0

        # Tokenizzazione e inferenza del modello
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

        # Le probabilità delle classi:
        # probs[0] = positivo
        # probs[1] = neutro
        # probs[2] = negativo
        
        prob_positive = probs[0]
        prob_neutral = probs[1]
        prob_negative = probs[2]

        # Calcolo del punteggio di sentiment attenuato dalla neutralità
        # Questo punteggio varia ancora tra -1 e 1
        raw_sentiment_score = prob_positive - prob_negative
        attenuation_factor = 1 - prob_neutral
        
        attenuated_score = raw_sentiment_score * attenuation_factor
        
        # Scala il punteggio attenuato per farlo variare tra -0.2 e 0.2
        # Il fattore di scala è 0.2, poiché l'intervallo totale è 0.4 (da -0.2 a 0.2)
        final_scaled_score = attenuated_score * 0.1
        
        return round(float(final_scaled_score), 4)
        
    except Exception as e:
        sys.stderr.write(f"Error in compute_sentiment for text (first 50 chars): '{text[:50]}...': {e}\n")
        return 0.0

def contains_finance_keywords(text):
    if text is None:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in FINANCE_KEYWORDS)


def process_message(msg):
    try:
        data = json.loads(msg)
        
        text_for_sentiment = data.get("text", "")
        created_utc_input = data.get("created_utc", "") # Timestamp Unix come stringa

        # --- Gestione Timestamp Unix per Reddit ---
        timestamp_str_output = ""
        try:
            # Converte il timestamp Unix (stringa) in float e poi in datetime
            unix_timestamp_float = float(created_utc_input)
            timestamp_obj = datetime.fromtimestamp(unix_timestamp_float, tz=timezone.utc)
            
            # Formatta in yyyy-MM-dd'T'HH:mm:ss'Z' (senza millisecondi)
            timestamp_str_output = timestamp_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        except (ValueError, TypeError) as e:
            sys.stderr.write(f"WARN: Could not parse Unix timestamp '{created_utc_input}' for Reddit: {e}. Using current UTC time.\n")
            timestamp_str_output = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            sys.stderr.write(f"ERROR: Unexpected error during timestamp parsing for Reddit '{created_utc_input}': {e}. Using current UTC time.\n")
            timestamp_str_output = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')


        # --- Filtro per parole chiave finanziarie (come nel codice Spark, ma non applicato di default) ---
        # Se vuoi applicare il filtro, dovrai aggiungere un .filter() dopo la map nel main()
        # if not contains_finance_keywords(text_for_sentiment):
        #     sys.stderr.write(f"DEBUG: Skipping Reddit post due to no finance keywords: {text_for_sentiment[:50]}...\n")
        #     return None # Restituisce None per filtrare il messaggio

        tickers = extract_tickers(text_for_sentiment)
        score = compute_sentiment(text_for_sentiment)

        out = {
            "timestamp": timestamp_str_output,
            "social": "reddit", # <--- Hardcodato a "reddit"
            "ticker": tickers, # extract_tickers restituisce già una lista
            "sentiment_score": score
        }

        return json.dumps(out)
    except Exception as e:
        sys.stderr.write(f"Failed to process message for Reddit: {e}\n")
        return json.dumps({
            "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "social": "reddit",
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
            "group.id": "reddit-sentiment-flink-group", # <--- Gruppo ID specifico
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
    
    # Se vuoi filtrare i messaggi senza parole chiave finanziarie, DECOMMENTA la riga seguente:
    # processed_stream = processed_stream.filter(lambda x: x is not None)

    # Add Kafka sink
    processed_stream.add_sink(kafka_producer)

    # Add console sink for debugging
    processed_stream.print()

    # Execute the Flink job
    print("Starting Flink job: Reddit Sentiment Analysis with Flink", file=sys.stderr)
    env.execute("Reddit Sentiment Analysis with Flink")
    print("Flink job finished.", file=sys.stderr)

if __name__ == "__main__":
    main()
