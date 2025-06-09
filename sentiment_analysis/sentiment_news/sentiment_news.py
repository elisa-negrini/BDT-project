import json
import re
import os
import sys
import numpy as np
from datetime import datetime, timezone
from scipy.special import softmax
import onnxruntime as ort
import psycopg2 # Import psycopg2 for database connection
from psycopg2 import OperationalError # Import to handle DB connection errors
import time # Import for retry logic

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from transformers import AutoTokenizer

# --- Kafka Configuration ---
SOURCE_TOPIC = "finnhub" # Source topic for news
TARGET_TOPIC = "news_sentiment" # Target topic for news sentiment
KAFKA_BROKER = "kafka:9092"

# --- PostgreSQL Database Configuration ---
# Ensure these environment variables are defined
# in your Docker environment or system (e.g., in docker-compose.yml).
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# --- Function for Database Connection with Retries ---
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

# --- Function to Load Ticker Map from Database ---
def load_company_ticker_map_from_db():
    """
    Loads ticker and company name information from the database
    and creates a dynamic map.
    """
    company_ticker_map = {}
    conn = None # Initialize to None for error handling
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

            # Add the main company name to the map
            if company_name:
                company_ticker_map[company_name.lower()] = ticker

            # Add related words (if present and not null)
            if related_words_str:
                # Assuming related_words are comma-separated (e.g., "Facebook, Meta Platforms")
                related_words = [w.strip() for w in related_words_str.split(',') if w.strip()]
                for word in related_words:
                    company_ticker_map[word.lower()] = ticker
        
        sys.stderr.write(f"INFO: COMPANY_TICKER_MAP loaded from DB with {len(company_ticker_map)} entries.\n")
        return company_ticker_map

    except Exception as e:
        sys.stderr.write(f"CRITICAL ERROR: Failed to load COMPANY_TICKER_MAP from database: {e}\n")
        # In case of critical DB error, you might want to exit or retry the application
        sys.exit(1) # Exit the program if essential data cannot be loaded
    finally:
        if conn:
            conn.close()

# --- Load the map at script startup ---
COMPANY_TICKER_MAP = load_company_ticker_map_from_db()


# Global model and tokenizer (lazy loaded)
finbert_tokenizer = None
finbert_session = None

def load_finbert_model():
    """
    Loads the FinBERT tokenizer and ONNX session.
    """
    global finbert_tokenizer, finbert_session
    
    # Get the base model path from the environment variable, or use /model as default
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
    """
    Extracts tickers from the given text based on '$TICKER' format,
    exact ticker symbols, or mentions of company names/related words loaded from DB.
    Returns a list of unique tickers found.
    """
    tickers = set()
    text_upper = text.upper()
    text_lower = text.lower() # Also need lowercase for company name matching

    # 1. Look for $TICKER format (e.g., $AAPL)
    matches_dollar = re.findall(r"\$([A-Z]{1,5})", text_upper)
    # Get all valid tickers from the values of COMPANY_TICKER_MAP
    valid_db_tickers = set(COMPANY_TICKER_MAP.values()) 
    for sym in matches_dollar:
        if sym in valid_db_tickers:
            tickers.add(sym)
    
    # 2. Look for explicit ticker symbols as standalone words (e.g., "AAPL" in text)
    # This regex finds sequences of 1-5 uppercase letters that are whole words.
    words = re.findall(r'\b[A-Z]{1,5}\b', text_upper) 
    for word in words:
        if word in valid_db_tickers:
            tickers.add(word)

    # 3. Look for company names or related words (loaded from DB)
    for company_name_or_keyword, ticker in COMPANY_TICKER_MAP.items():
        # Use word boundary for more precise matching of company names/keywords
        # For simplicity, we are doing a substring check (name in lowered)
        # For more robustness, consider re.search(r'\b' + re.escape(company_name_or_keyword) + r'\b', text_lower)
        if company_name_or_keyword in text_lower:
            tickers.add(ticker)
            
    return list(tickers)


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


def process_message(msg):
    """
    Processes a single Kafka message (expected to be news data):
    parses JSON, extracts headline, summary, and timestamp,
    determines associated tickers, and computes sentiment score.
    Returns a JSON string of the processed output or an error message.
    """
    try:
        data = json.loads(msg)
        
        # --- Extracting specific fields for news data ---
        headline = data.get("headline", "")
        summary = data.get("summary", "")
        symbol_requested = data.get("symbol_requested", "UNKNOWN") # Ticker as reported by the news source
        created_at_input = data.get("date", "") # Timestamp field for news is 'date'
        
        # Concatenate headline and summary for sentiment analysis
        text_for_sentiment = f"{headline} {summary}".strip()

        # --- Timestamp Handling ---
        # Assuming 'date' is already in a suitable format for the consumer.
        # If the consumer needs further parsing, re-introduce the robust parsing logic from the Bluesky script.
        timestamp_str_output = created_at_input 
        
        # Extract tickers from the combined text and compute sentiment
        tickers = extract_tickers(text_for_sentiment)
        score = compute_sentiment(text_for_sentiment)

        # Prepare output JSON structure
        # Combine extracted tickers with the 'symbol_requested' from the news payload
        # Ensure unique tickers and convert to list
        combined_tickers = set(tickers)
        if symbol_requested != "UNKNOWN":
            combined_tickers.add(symbol_requested)

        out = {
            "timestamp": timestamp_str_output,
            "social": "news", # Hardcoded source as "news"
            "ticker": list(combined_tickers),
            "sentiment_score": score
        }

        return json.dumps(out)
    except Exception as e:
        sys.stderr.write(f"ERROR: Failed to process message for news: {e}\n")
        # Return a structured error message to Kafka if processing fails
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(timespec='milliseconds') + 'Z',
            "social": "news",
            "ticker": ["ERROR"], # Indicate an error for ticker
            "sentiment_score": 0.0, # Default sentiment score
            "error_message": str(e),
            "original_message": msg # Include original message for debugging
        })

def main():
    """
    Main function to set up and execute the Flink stream processing job.
    It reads news data from a Kafka source, processes messages,
    and writes sentiment analysis results to a Kafka sink.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Set parallelism for the Flink job

    # Kafka source configuration
    kafka_consumer = FlinkKafkaConsumer(
        topics=SOURCE_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "news-sentiment-flink-group", # Specific group ID for news sentiment
            "auto.offset.reset": "earliest" # Start reading from the beginning of the topic if no offset is committed
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