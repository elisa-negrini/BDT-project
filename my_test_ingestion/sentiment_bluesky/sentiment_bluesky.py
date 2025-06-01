# import os
# import sys
# import json
# import re
# import onnxruntime as ort
# import numpy as np
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, col, from_json, current_timestamp, date_format, to_timestamp, when, lit
# from pyspark.sql.types import StringType, StructType, StructField
# from transformers import AutoTokenizer, AutoModelForSequenceClassification, BertTokenizer
# from scipy.special import softmax
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# # 1. Spark Session Initialization
# # We'll set a higher log level for cleaner output and configure executor/driver memory
# # to better handle the FinBERT model.
# spark = SparkSession.builder \
#     .appName("SentimentBluesky") \
#     .master("spark://spark-master:7077") \
#     .config("spark.sql.session.timeZone", "UTC") \
#     .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("WARN") 

# # 2. Kafka Configuration
# SOURCE_TOPIC = "bluesky"
# TARGET_TOPIC = "bluesky_sentiment"
# KAFKA_BROKER = "kafka:9092"

# # 3. Company Name to Ticker Mapping
# COMPANY_TICKER_MAP = {
#     "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN", "nvidia": "NVDA",
#     "meta": "META", "facebook": "META", "oracle": "ORCL", "tesla": "TSLA", "unitedhealth": "UNH",
#     "johnson & johnson": "JNJ", "visa": "V", "exxon": "XOM", "procter & gamble": "PG",
#     "mastercard": "MA", "broadcom": "AVGO", "lilly": "LLY", "jpmorgan": "JPM", "home depot": "HD",
#     "chevron": "CVX", "mrk": "MRK", "pepsico": "PEP", "coca cola": "KO", "abbvie": "ABBV",
#     "costco": "COST", "adobe": "ADBE", "walmart": "WMT", "bank of america": "BAC",
#     "salesforce": "CRM", "mcdonald": "MCD", "thermo fisher": "TMO", "ibm" : "IBM"
# }


# # Global tokenizer/session for Spark UDF
# finbert_tokenizer = None
# finbert_session = None

# def compute_sentiment_onnx(text):
#     global finbert_tokenizer, finbert_session

#     try:
#         if not text or text.strip() == "":
#             return "0.0" # Return neutral for empty or whitespace-only text

#         # Lazy load tokenizer and ONNX session using absolute paths
#         if finbert_tokenizer is None:
#             # Correct path to the tokenizer directory inside /app/model/
#             tokenizer_path = "model/tokenizer"
#             if not os.path.isdir(tokenizer_path):
#                 # Add some logging if path doesn't exist
#                 sys.stderr.write(f"ERROR: Tokenizer path not found: {tokenizer_path}\n")
#                 # Fallback or raise an error that Spark can better report
#                 # For now, let it try and fail, but logging helps debug
#             finbert_tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)

#         if finbert_session is None:
#             # Correct path to the ONNX model file inside /app/model/
#             # Ensure this is the correct name of your .onnx file
#             model_path = "/app/model/model.onnx" # Or "/app/model/finbert-model-quantized.onnx" if that's the actual name
#             if not os.path.exists(model_path):
#                 sys.stderr.write(f"ERROR: Model path not found: {model_path}\n")
#             finbert_session = ort.InferenceSession(model_path)

#         # Tokenize input text
#         tokens = finbert_tokenizer(
#             text,
#             return_tensors="np",
#             truncation=True,
#             padding="max_length",
#             max_length=128 # You can adjust this
#         )

#         ort_inputs = {k: v for k, v in tokens.items()}
#         outputs = finbert_session.run(None, ort_inputs)
#         logits = outputs[0]

#         # FinBERT uses 3 classes: [positive, neutral, negative]
#         probs = softmax(logits[0])
#         score = float(probs[0] - probs[2]) # positive - negative

#         return str(round(score, 4))

#     except Exception as e:
#         import traceback
#         # Print to stderr so it's more likely to appear in Spark worker logs
#         sys.stderr.write(f"Error in ONNX sentiment processing text (first 50 chars): '{text[:50]}...'\n")
#         sys.stderr.write(f"Exception type: {type(e)}\n")
#         sys.stderr.write(f"Exception message: {e}\n")
#         sys.stderr.write(f"Traceback: {traceback.format_exc()}\n")
#         return "0.0" # Return a neutral value on error



# # 4. Lazy-load FinBERT Model
# # These are global variables that will be initialized the first time get_finbert_sentiment is called.
# # tokenizer = None
# # model = None

# # def get_finbert_sentiment(text):
# #     """
# #     Calculates the sentiment score using the FinBERT model.
# #     The model and tokenizer are loaded only once per executor process.
# #     Returns the score as a string to be compatible with Spark UDF return types.
# #     """
# #     try:
# #         global tokenizer, model
# #         if tokenizer is None or model is None:
# #             # Load the tokenizer and model on the first call
# #             tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
# #             model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        
# #         # Prepare text for the model
# #         inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
        
# #         # Perform inference without gradient calculation
# #         with torch.no_grad():
# #             logits = model(**inputs).logits
        
# #         # Apply softmax to get probabilities and calculate sentiment score
# #         probs = softmax(logits.numpy()[0])
# #         sentiment_score = float(probs[0] - probs[2])  # positive - negative
# #         return str(sentiment_score)
# #     except Exception as e:
# #         # Log the full traceback for detailed debugging
# #         import traceback
# #         print(f"Error calculating sentiment for text (first 100 chars): '{text[:100]}...': {e}\n{traceback.format_exc()}", file=sys.stderr)
# #         return "0.0" # Return a neutral value on error

# # 5. Ticker Extraction from Text
# def extract_tickers(text):
#     """
#     Extracts relevant stock tickers from text, based on '$TICKER' symbols
#     and company names found in COMPANY_TICKER_MAP.
#     If no specific tickers are found, it assigns "GENERAL".
#     Returns the list of tickers as a JSON string.
#     """
#     tickers = set()
    
#     # Extract tickers starting with "$"
#     matches = re.findall(r"\$([A-Z]{1,5})", text.upper())
#     for sym in matches:
#         if sym in COMPANY_TICKER_MAP.values(): # Only add if it's a known ticker
#             tickers.add(sym)
            
#     # Extract tickers based on company names
#     lowered = text.lower()
#     for name, ticker in COMPANY_TICKER_MAP.items():
#         if name in lowered:
#             tickers.add(ticker)
            
#     # If no specific tickers are found, assign "GENERAL"
#     return json.dumps(list(tickers) if tickers else ["GENERAL"])

# # 6. Kafka Payload Creation
# def to_kafka_payload(tickers_json, sentiment_score_str, timestamp):
#     """
#     Creates the JSON payload string to be sent to Kafka.
#     It unifies the variable names according to the consumer's expectations.
#     """
#     try:
#         return json.dumps({
#             "timestamp": timestamp,
#             "social": "bluesky", # Source is hardcoded as 'bluesky'
#             "ticker": json.loads(tickers_json), # Ticker is expected as a list
#             "sentiment_score": float(sentiment_score_str) # Unified field name
#         })
#     except Exception as e:
#         print(f"Error creating Kafka payload: {e}. Tickers: {tickers_json}, Sentiment: {sentiment_score_str}, Timestamp: {timestamp}", file=sys.stderr)
#         # Return a fallback JSON with error info if serialization fails
#         return json.dumps({
#             "timestamp": timestamp if timestamp else "unknown",
#             "social": "bluesky",
#             "ticker": ["ERROR"],
#             "sentiment_score": 0.0,
#             "error": str(e)
#         })


# # 7. UDF Registrations
# extract_tickers_udf = udf(extract_tickers, StringType())
# get_sentiment_udf = udf(compute_sentiment_onnx, StringType())
# to_kafka_row_udf = udf(to_kafka_payload, StringType())

# # 8. Input Data Schema from Kafka
# schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("text", StringType(), True),
#     StructField("user", StringType(), True),
#     StructField("created_at", StringType(), True) 
# ])

# # 9. Read Stream from Kafka
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", SOURCE_TOPIC) \
#     .option("startingOffsets", "latest") \
#     .load()

# # 10. Parsing, Sentiment, Ticker Extraction, and Timestamp Handling
# df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json("json_str", schema).alias("data")) \
#     .select("data.*")

# # Define a list of possible timestamp formats to try for 'created_at'
# # The order matters: try the most specific/common first.
# timestamp_formats = [
#     "yyyy-MM-dd'T'HH:mm:ss.SSSX",  # e.g., 2025-05-28T19:47:32.771Z or +01:00
#     "yyyy-MM-dd'T'HH:mm:ssX",      # e.g., 2025-05-28T19:47:32Z (no milliseconds)
#     "yyyy-MM-dd'T'HH:mm:ss.SSS",   # e.g., 2025-05-28T19:47:32.771 (no timezone info)
#     "yyyy-MM-dd'T'HH:mm:ss",       # e.g., 2025-05-28T19:47:32 (no milliseconds, no timezone)
#     "yyyy-MM-dd HH:mm:ss.SSS",     # e.g., 2025-05-28 19:47:32.771 (space instead of 'T')
#     "yyyy-MM-dd HH:mm:ss"          # e.g., 2025-05-28 19:47:32 (space, no milliseconds)
# ]

# # Create a temporary column to store the parsed timestamp, trying each format
# parsed_ts_col = lit(None).cast("timestamp")
# for fmt in timestamp_formats:
#     parsed_ts_col = when(parsed_ts_col.isNull(), to_timestamp(col("created_at"), fmt)).otherwise(parsed_ts_col)

# df_processed = df_parsed \
#     .withColumn("parsed_created_at", parsed_ts_col) \
#     .filter(col("parsed_created_at").isNotNull()) \
#     .withColumn("tickers", extract_tickers_udf(col("text"))) \
#     .withColumn("sentiment_score_val", get_sentiment_udf(col("text"))) \
#     .withColumn("timestamp", date_format(col("parsed_created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
#     .withColumn("value", to_kafka_row_udf(
#         col("tickers"), col("sentiment_score_val"), col("timestamp"))
#     )

# # 11. Write to Kafka (Final Result)
# query_kafka = df_processed.selectExpr("CAST(value AS STRING) as value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("topic", TARGET_TOPIC) \
#     .option("checkpointLocation", "/tmp/checkpoints/bluesky_sentiment_kafka") \
#     .start()

# # 12. Print to Console for Debugging (Optional, but highly recommended during development)
# query_console = df_processed.select("created_at", "parsed_created_at", "timestamp", "tickers", "sentiment_score_val", "value") \
#     .writeStream \
#     .format("console") \
#     .option("truncate", False) \
#     .option("numRows", 10) \
#     .start()

# # 13. Await Stream Termination
# spark.streams.awaitAnyTermination()

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
SOURCE_TOPIC = "bluesky"
TARGET_TOPIC = "bluesky_sentiment"
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

# Global model and tokenizer (lazy loaded in a more robust way)
finbert_tokenizer = None
finbert_session = None

def load_finbert_model():
    """
    Loads the FinBERT tokenizer and ONNX session.
    This function will be called once per worker/process.
    """
    global finbert_tokenizer, finbert_session
    if finbert_tokenizer is None:
        try:
            # Paths are relative to the working_dir /app
            tokenizer_path = "/app/model/tokenizer"
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
            # Path for the ONNX model file
            model_path = "/app/model/model.onnx"
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
        text = data.get("text", "")
        created_at = data.get("created_at", "")

        timestamp_str_parsed = ""
        try:
            match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z)?", created_at)
            if match:
                base_time = match.group(1)
                fractional_seconds = match.group(2)
                timezone_z = match.group(3)

                if fractional_seconds:
                    fractional_seconds = fractional_seconds[:7]
                else:
                    fractional_seconds = ""

                timestamp_to_parse = f"{base_time}{fractional_seconds}{timezone_z if timezone_z else ''}"
                
                timestamp_obj = datetime.fromisoformat(timestamp_to_parse.replace("Z", "+00:00"))
                
                timestamp_obj_utc = timestamp_obj.astimezone(timezone.utc)
                timestamp_str_parsed = timestamp_obj_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            else:
                raise ValueError("Timestamp format not recognized by regex.")
        except ValueError as ve:
            sys.stderr.write(f"WARN: Could not parse timestamp '{created_at}' due to format error: {ve}. Using current UTC time.\n")
            timestamp_str_parsed = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
        except Exception as e:
            sys.stderr.write(f"ERROR: Unexpected error during timestamp parsing for '{created_at}': {e}. Using current UTC time.\n")
            timestamp_str_parsed = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'

        tickers = extract_tickers(text)
        score = compute_sentiment(text)

        out = {
            "timestamp": timestamp_str_parsed,
            "social": "bluesky",
            "ticker": tickers,
            "sentiment_score": score
        }

        return json.dumps(out)
    except Exception as e:
        sys.stderr.write(f"Failed to process message: {e}\n")
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(timespec='milliseconds') + 'Z',
            "social": "bluesky",
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
            "group.id": "sentiment-flink-group",
            "auto.offset.reset": "earliest"
        }
    )

    # Kafka sink (your primary output)
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

    # --- INIZIO NUOVA MODIFICA PER LA CONSOLE SINK ---
    # Aggiungi una console sink per stampare i messaggi processati sul log del container
    processed_stream.print()
    # --- FINE NUOVA MODIFICA PER LA CONSOLE SINK ---

    # Execute the Flink job
    print("Starting Flink job: Sentiment Analysis with Flink", file=sys.stderr)
    env.execute("Sentiment Analysis with Flink")
    print("Flink job finished.", file=sys.stderr)

if __name__ == "__main__":
    main()


