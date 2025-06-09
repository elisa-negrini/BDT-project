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
SOURCE_TOPIC = "bluesky"
TARGET_TOPIC = "bluesky_sentiment"
KAFKA_BROKER = "kafka:9092"

# --- PostgreSQL Database Configuration ---
# Ensure these environment variables are defined
# in your Docker environment or system.
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

            # Add the main company name
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


# Global model and tokenizer (lazy loaded in a more robust way)
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
    Extracts tickers from the given text based on '$TICKER' format
    or mentions of company names/related words.
    Returns a list of unique tickers found, or ['GENERAL'] if none are identified.
    """
    tickers = set()
    # Find tickers like $AAPL
    matches = re.findall(r"\$([A-Z]{1,5})", text.upper())
    for sym in matches:
        # Check if the extracted symbol is in our valid ticker list (values of COMPANY_TICKER_MAP)
        if sym in COMPANY_TICKER_MAP.values():
            tickers.add(sym)
    
    # Find company names or related words
    lowered = text.lower()
    for name, ticker in COMPANY_TICKER_MAP.items():
        # Check if the company name/related word is present in the text
        if name in lowered: # Simple substring check for now, can be improved with word boundaries if needed
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


def process_message(msg):
    """
    Processes a single Kafka message: parses JSON, extracts text and timestamp,
    determines associated tickers, and computes sentiment score.
    Returns a JSON string of the processed output or an error message.
    """
    try:
        data = json.loads(msg)
        text = data.get("text", "")
        created_at = data.get("created_at", "")

        # --- Timestamp Parsing Logic ---
        timestamp_str_parsed = ""
        try:
            # Regex to handle timestamps with optional microseconds and 'Z' timezone
            match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z)?", created_at)
            if match:
                base_time = match.group(1)
                fractional_seconds = match.group(2)
                timezone_z = match.group(3)

                # Truncate fractional seconds to milliseconds if present
                if fractional_seconds:
                    fractional_seconds = fractional_seconds[:7] # keep up to 6 digits (microseconds)
                else:
                    fractional_seconds = ""

                # Construct the string for datetime.fromisoformat
                timestamp_to_parse = f"{base_time}{fractional_seconds}{timezone_z if timezone_z else ''}"
                
                # Parse and convert to UTC
                timestamp_obj = datetime.fromisoformat(timestamp_to_parse.replace("Z", "+00:00"))
                timestamp_obj_utc = timestamp_obj.astimezone(timezone.utc)
                
                # Format to desired string (milliseconds and 'Z' for UTC)
                timestamp_str_parsed = timestamp_obj_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            else:
                raise ValueError("Timestamp format not recognized by regex.")
        except ValueError as ve:
            sys.stderr.write(f"WARN: Could not parse timestamp '{created_at}' due to format error: {ve}. Using current UTC time.\n")
            timestamp_str_parsed = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
        except Exception as e:
            sys.stderr.write(f"ERROR: Unexpected error during timestamp parsing for '{created_at}': {e}. Using current UTC time.\n")
            timestamp_str_parsed = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'

        # Extract tickers and compute sentiment
        tickers = extract_tickers(text)
        score = compute_sentiment(text)

        # Prepare output JSON
        out = {
            "timestamp": timestamp_str_parsed,
            "social": "bluesky",
            "ticker": tickers,
            "sentiment_score": score
        }

        return json.dumps(out)
    except Exception as e:
        sys.stderr.write(f"ERROR: Failed to process message: {e}\n")
        # Return a structured error message to Kafka if processing fails
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(timespec='milliseconds') + 'Z',
            "social": "bluesky",
            "ticker": ["ERROR"], # Indicate an error for ticker
            "sentiment_score": 0.0, # Default sentiment score
            "error_message": str(e),
            "original_message": msg # Include original message for debugging
        })

def main():
    """
    Main function to set up and execute the Flink stream processing job.
    It reads from a Kafka source, processes messages, and writes results to a Kafka sink.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Set parallelism for the Flink job

    # Kafka source configuration
    kafka_consumer = FlinkKafkaConsumer(
        topics=SOURCE_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "sentiment-flink-group",
            "auto.offset.reset": "earliest" # Start reading from the beginning of the topic if no offset is committed
        }
    )

    # Kafka sink (your primary output) configuration
    kafka_producer = FlinkKafkaProducer(
        topic=TARGET_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BROKER}
    )

    # Stream processing pipeline
    processed_stream = env.add_source(kafka_consumer) \
        .map(lambda msg: process_message(msg), output_type=Types.STRING())
    
    # Add Kafka sink to send processed messages to TARGET_TOPIC
    processed_stream.add_sink(kafka_producer)

    # Add a console sink to print processed messages to the container's log
    processed_stream.print()

    # Execute the Flink job
    sys.stderr.write("INFO: Starting Flink job: Sentiment Analysis with Flink\n")
    env.execute("Sentiment Analysis with Flink")
    sys.stderr.write("INFO: Flink job finished.\n")

if __name__ == "__main__":
    main()