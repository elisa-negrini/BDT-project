from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, date
import json
import os
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import logging
import time
import pytz
from threading import Timer
import psycopg2

# === CONFIGURATION === 
# Alpaca API credentials - MUST be set as environment variables
API_KEY_ALPACA = os.getenv("API_KEY_ALPACA") 
API_SECRET_ALPACA = os.getenv("API_SECRET_ALPACA")

# Kafka broker configuration - MUST be set as an environment variable
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "h_alpaca" # Kafka topic for historical data

# Database connection details for fetching tickers - MUST be set as environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Date range for historical data download
START_DATE = datetime(2021, 1, 1)
END_DATE = datetime(2025, 5, 14) # Note: Adjust END_DATE as needed for actual data availability

# Checkpoint file to track processed data
CHECKPOINT_FILE = "checkpoint.json"

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === Custom JSON Encoder for datetime objects ===
class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime, date, and pandas Timestamp objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)

# === Kafka Connection Function ===
def connect_kafka():
    """Establishes and returns a KafkaProducer connection, with retry logic."""
    # Validate Kafka broker config
    if not KAFKA_BOOTSTRAP_SERVERS:
        logger.critical("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
        sys.exit(1)

    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all', # Ensure all replicas have acknowledged the message
                retries=3,  # Retry failed message sends
                batch_size=16384, # Buffer messages to send in batches of 16KB
                linger_ms=10, # Wait up to 10ms to fill a batch before sending
                buffer_memory=33554432 # Total buffer memory for unsent records
            )
            logger.info("Successfully connected to Kafka.")
            return producer
        except Exception as e:
            logger.warning(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

# === Checkpoint Functions ===
def load_checkpoint():
    """Loads the checkpoint file to resume processing. Returns an empty dict if not found or on error."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not read checkpoint file: {e}. Starting fresh.")
    return {}

def save_checkpoint(cp):
    """Saves the current checkpoint state to a file."""
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(cp, f, indent=2)
    except Exception as e:
        logger.error(f"Error writing checkpoint file: {e}")

# === Database Ticker Fetching Function ===
def fetch_tickers_from_db():
    """
    Fetches active stock tickers from the PostgreSQL database.
    Includes retry logic for database connection.
    Exits if persistent connection failure occurs.
    """
    # Validate database config
    if not all([POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
        logger.critical("One or more PostgreSQL environment variables (HOST, PORT, DB, USER, PASSWORD) are not set. Exiting.")
        sys.exit(1)

    max_retries = 15 # Increased retries as DB might take longer to start
    initial_delay = 5
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            
            # Attempt to select active tickers; fallback if 'is_active' column doesn't exist
            try:
                cursor.execute("SELECT DISTINCT ticker FROM companies_info WHERE is_active = TRUE;")
            except psycopg2.ProgrammingError:
                logger.warning("Column 'is_active' not found in 'companies_info'. Falling back to fetching all distinct tickers.")
                cursor.execute("SELECT DISTINCT ticker FROM companies_info;")
            
            result = cursor.fetchall()
            tickers = [row[0] for row in result if row[0] is not None]
            cursor.close()
            conn.close()
            
            if not tickers:
                logger.warning("No active tickers found in the database. Ensure 'companies_info' table is populated and 'is_active' flags are set, or the table exists.")
            else:
                logger.info(f"Fetched {len(tickers)} active tickers from DB.")
            return tickers
        except psycopg2.OperationalError as e:
            logger.warning(f"Database not available, retrying in {initial_delay * (i + 1)} seconds... ({e})")
            time.sleep(initial_delay * (i + 1)) # Exponential backoff
        except Exception as e:
            logger.error(f"Error fetching tickers from database: {e}")
            # Do not exit immediately on general error, allow retry
            time.sleep(initial_delay * (i + 1)) # Wait before next retry
    logger.critical(f"Failed to connect to database after {max_retries} attempts. Exiting.")
    sys.exit(1) # Exit if database connection fails persistently

# Fetch tickers from the database on script start
TICKERS_FROM_DB = fetch_tickers_from_db()

# Exit if no tickers are found from the database, as the script cannot proceed meaningfully
if not TICKERS_FROM_DB:
    logger.critical("No tickers available from the database. Exiting application.")
    sys.exit(1)

# === Download Data for a Ticker Function ===
def download_ticker_data(ticker):
    """Downloads historical bar data for a given ticker from Alpaca."""
    # Validate Alpaca API credentials
    if not API_KEY_ALPACA or not API_SECRET_ALPACA:
        logger.critical("Alpaca API_KEY_ALPACA or API_SECRET_ALPACA environment variables are not set. Exiting.")
        sys.exit(1)

    logger.info(f"Downloading data for {ticker}...")

    client = StockHistoricalDataClient(API_KEY_ALPACA, API_SECRET_ALPACA)
    req = StockBarsRequest(
        symbol_or_symbols=ticker,
        timeframe=TimeFrame.Minute,
        start=START_DATE,
        end=END_DATE
    )

    try:
        bars = client.get_stock_bars(req).df
        if bars.empty:
            logger.warning(f"No data available for {ticker} in the specified range.")
            return None

        # Process and filter data for market hours
        df = bars.reset_index()
        df = df.set_index('timestamp')
        df = df.tz_convert("America/New_York") # Convert to NY timezone for market hours filtering
        df = df.between_time("09:30", "16:00") # Filter for market hours (9:30 AM to 4:00 PM ET)
        df = df.tz_convert("UTC").reset_index() # Convert back to UTC for consistency
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True) # Ensure timestamp is proper datetime object
        logger.info(f"Downloaded {len(df)} filtered records for {ticker}")
        return df

    except Exception as e:
        logger.error(f"Error downloading data for {ticker}: {e}")
        return None

# === Send Data to Kafka Function ===
def send_data_to_kafka(producer, ticker, df, checkpoint):
    """Sends historical data to Kafka, grouped by day, and updates checkpoint."""
    if df is None or df.empty:
        return

    df['date'] = df['timestamp'].dt.date
    grouped = df.groupby('date')

    for date_val, group_df in grouped:
        date_str = str(date_val)
        if date_str in checkpoint.get(ticker, []):
            logger.info(f"Day {ticker} {date_str} already processed, skipping.")
            continue

        message = {
            'ticker': ticker,
            'date': date_str,
            'year': date_val.year,
            'month': date_val.month,
            'data': group_df.drop('date', axis=1).to_dict('records') # Convert DataFrame group to list of dicts
        }

        try:
            # Using ticker_date_str as key for partitioning and ordering
            future = producer.send(
                topic=KAFKA_TOPIC,
                key=f"{ticker}_{date_str}",
                value=message
            )
            future.get(timeout=10) # Block until message is sent (or timeout) to ensure delivery
            logger.info(f"Sent {len(group_df)} records for {ticker} on {date_str}")
            
            # Update checkpoint and save after successful send
            checkpoint.setdefault(ticker, []).append(date_str)
            save_checkpoint(checkpoint)
            reset_timer() # Reset inactivity timer on successful data processing

        except KafkaError as e:
            logger.error(f"KafkaError sending data for {ticker} on {date_str}: {e}")
        except Exception as e:
            logger.error(f"Generic error sending data for {ticker} on {date_str}: {e}")


# === Inactivity Timer Functions ===
inactivity_timer = None
TIMEOUT_SECONDS = 300 # 5 minutes of inactivity before exiting

def timeout_exit():
    """Function called if no data is processed for a certain period."""
    logger.warning(f"No data processed in the last {TIMEOUT_SECONDS} seconds. Shutting down producer.")
    os._exit(0) # Force exit if stuck

def reset_timer():
    """Resets or starts the inactivity timer."""
    global inactivity_timer
    if inactivity_timer:
        inactivity_timer.cancel() # Cancel existing timer if any
    inactivity_timer = Timer(TIMEOUT_SECONDS, timeout_exit) # Start a new timer
    inactivity_timer.start()

# === Main Execution Function ===
def main():
    """Main function to orchestrate historical data download and Kafka ingestion."""
    global checkpoint
    logger.info("Starting Alpaca Historical Data Producer -> Kafka pipeline.")
    producer = connect_kafka() # Establish Kafka connection
    checkpoint = load_checkpoint() # Load processing checkpoint

    reset_timer() # Start inactivity timer as the script begins

    try:
        processed_tickers_count = 0
        failed_tickers_count = 0

        for ticker in TICKERS_FROM_DB: # Iterate over dynamically fetched tickers
            try:
                # Get all business days within the specified date range
                expected_dates = pd.date_range(START_DATE.date(), END_DATE.date(), freq='B').strftime("%Y-%m-%d").tolist()
                done_dates = set(checkpoint.get(ticker, []))
                
                # If all expected dates for this ticker are already processed, skip
                if all(d in done_dates for d in expected_dates):
                    logger.info(f"Ticker {ticker} already fully processed, skipping.")
                    processed_tickers_count += 1
                    continue

                # Download data for the ticker
                df = download_ticker_data(ticker)
                if df is not None:
                    # Send downloaded data to Kafka
                    send_data_to_kafka(producer, ticker, df, checkpoint)
                    processed_tickers_count += 1
                else:
                    failed_tickers_count += 1

            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {e}", exc_info=True) # Log full traceback
                failed_tickers_count += 1

        producer.flush() # Ensure all buffered messages are sent before concluding
        logger.info(f"Processing complete! Successful tickers: {processed_tickers_count}, Errors/Skipped: {failed_tickers_count}")

    except KeyboardInterrupt:
        logger.info("User interruption detected. Shutting down gracefully.")
    except Exception as e:
        logger.critical(f"Fatal error occurred during main execution: {e}", exc_info=True)
    finally:
        # Ensure inactivity timer is cancelled before final shutdown
        if inactivity_timer:
            inactivity_timer.cancel()
        
        save_checkpoint(checkpoint) # Always save final checkpoint
        if producer:
            logger.info("Closing Kafka producer connection.")
            producer.close() # Close Kafka producer
        logger.info("Producer terminated.")

if __name__ == "__main__":
    main()
