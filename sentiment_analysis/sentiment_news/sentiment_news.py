import json
import re
import os
import sys
import numpy as np
from datetime import datetime, timezone
from scipy.special import softmax
import onnxruntime as ort
import psycopg2
from psycopg2 import OperationalError
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from transformers import AutoTokenizer

# ==== Kafka Configuration ====
SOURCE_TOPIC = "finnhub"
TARGET_TOPIC = "news_sentiment"
KAFKA_BROKER = "kafka:9092"

# ==== PostgreSQL Database Configuration ====
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def connect_to_db_for_keywords(max_retries=15, delay=5):
    """Attempts to connect to the database with retry logic."""
    db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(db_url)
            sys.stderr.write(f"INFO: Connected to PostgreSQL successfully (attempt {i+1}).\n")
            return conn
        except OperationalError as e:
            sys.stderr.write(f"ERROR: PostgreSQL connection failed: {e}. Retrying in {delay}s...\n")
        except Exception as e:
            sys.stderr.write(f"ERROR: Unexpected error during DB connection: {e}. Retrying in {delay}s...\n")
        time.sleep(delay)
    sys.stderr.write("CRITICAL ERROR: Max retries reached. Could not connect to PostgreSQL.\n")
    raise Exception("Failed to connect to database.")

def load_company_ticker_map_from_db():
    """
    Loads ticker and company name information from the database
    and creates a dynamic map.
    """
    company_ticker_map = {}
    conn = None
    try:
        conn = connect_to_db_for_keywords()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ticker, company_name, related_words
            FROM companies_info
            WHERE is_active = TRUE
        """)
        rows = cursor.fetchall()
        cursor.close()

        for row in rows:
            ticker, company_name, related_words_str = row

            if company_name:
                company_ticker_map[company_name.lower()] = ticker

            if related_words_str:
                related_words = [w.strip() for w in related_words_str.split(',') if w.strip()]
                for word in related_words:
                    company_ticker_map[word.lower()] = ticker
        
        sys.stderr.write(f"INFO: COMPANY_TICKER_MAP loaded from DB with {len(company_ticker_map)} entries.\n")
        return company_ticker_map

    except Exception as e:
        sys.stderr.write(f"CRITICAL ERROR: Failed to load COMPANY_TICKER_MAP from database: {e}\n")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

COMPANY_TICKER_MAP = load_company_ticker_map_from_db()

finbert_tokenizer = None
finbert_session = None

def load_finbert_model():
    """
    Loads the FinBERT tokenizer and ONNX Runtime session for sentiment analysis.
    This function ensures the model is loaded only once (lazy loading).
    """
    global finbert_tokenizer, finbert_session
    
    base_model_dir = "/model"

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
    """
    Extracts tickers from the given text based on '$TICKER' format,
    exact ticker symbols, or mentions of company names/related words loaded from DB.
    Returns a list of unique tickers found.
    """
    tickers = set()
    text_upper = text.upper()
    text_lower = text.lower()

    # 1. Look for $TICKER format (e.g., $AAPL)
    matches_dollar = re.findall(r"\$([A-Z]{1,5})", text_upper)
    valid_db_tickers = set(COMPANY_TICKER_MAP.values()) 
    for sym in matches_dollar:
        if sym in valid_db_tickers:
            tickers.add(sym)
    
    # 2. Look for explicit ticker symbols as standalone words (e.g., "AAPL" in text)
    words = re.findall(r'\b[A-Z]{1,5}\b', text_upper) 
    for word in words:
        if word in valid_db_tickers:
            tickers.add(word)

    # 3. Look for company names or related words (loaded from DB)
    for company_name_or_keyword, ticker in COMPANY_TICKER_MAP.items():
        if company_name_or_keyword in text_lower:
            tickers.add(ticker)
            
    return list(tickers)

def compute_sentiment(text):
    """
    Computes the sentiment score for the given text using the FinBERT model.
    The score is normalized to a range between -0.2 and 0.2.
    """
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

        prob_positive = probs[0]
        prob_neutral = probs[1]
        prob_negative = probs[2]

        raw_sentiment_score = prob_positive - prob_negative
        attenuation_factor = 1 - prob_neutral
        
        attenuated_score = raw_sentiment_score * attenuation_factor
    
        final_scaled_score = attenuated_score * 0.1
        
        return round(float(final_scaled_score), 4)
        
    except Exception as e:
        sys.stderr.write(f"Error in compute_sentiment for text (first 50 chars): '{text[:50]}...': {e}\n")
        return 0.0


def process_message(msg):
    """
    Processes a single Kafka message (expected to be news data):
    parses JSON, extracts headline, summary, and timestamp,
    determines associated tickers, and computes sentiment score.
    Returns a JSON string of the processed output or an error message.
    """
    try:
        data = json.loads(msg)
        
        headline = data.get("headline", "")
        summary = data.get("summary", "")
        symbol_requested = data.get("symbol_requested", "UNKNOWN")
        created_at_input = data.get("date", "")

        text_for_sentiment = f"{headline} {summary}".strip()

        # --- Timestamp Handling ---
        timestamp_str_output = created_at_input 

        tickers = extract_tickers(text_for_sentiment)
        score = compute_sentiment(text_for_sentiment)

        combined_tickers = set(tickers)
        if symbol_requested != "UNKNOWN":
            combined_tickers.add(symbol_requested)

        out = {
            "timestamp": timestamp_str_output,
            "social": "news",
            "ticker": list(combined_tickers),
            "sentiment_score": score
        }

        return json.dumps(out)
    except Exception as e:
        sys.stderr.write(f"ERROR: Failed to process message for news: {e}\n")
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(timespec='milliseconds') + 'Z',
            "social": "news",
            "ticker": ["ERROR"],
            "sentiment_score": 0.0,
            "error_message": str(e),
            "original_message": msg
        })

def main():
    """
    Main function to set up and execute the Flink stream processing job.
    It reads news data from a Kafka source, processes messages,
    and writes sentiment analysis results to a Kafka sink.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka source configuration
    kafka_consumer = FlinkKafkaConsumer(
        topics=SOURCE_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "news-sentiment-flink-group",
            "auto.offset.reset": "earliest"
        }
    )

    # Kafka sink (your primary output) configuration
    kafka_producer = FlinkKafkaProducer(
        topic=TARGET_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BROKER}
    )

    # Stream processing pipeline: Add source, map (process), and add sink
    processed_stream = env.add_source(kafka_consumer) \
        .map(lambda msg: process_message(msg), output_type=Types.STRING())
    
    # Add Kafka sink
    processed_stream.add_sink(kafka_producer)

    # Add console sink for debugging and logging processed messages
    processed_stream.print()

    # Execute the Flink job
    sys.stderr.write("INFO: Starting Flink job: News Sentiment Analysis with Flink\n")
    env.execute("News Sentiment Analysis with Flink")
    sys.stderr.write("INFO: Flink job finished.\n")

if __name__ == "__main__":
    main()