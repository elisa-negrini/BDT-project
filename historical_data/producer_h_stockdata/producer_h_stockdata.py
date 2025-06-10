# from alpaca.data.historical import StockHistoricalDataClient
# from alpaca.data.requests import StockBarsRequest
# from alpaca.data.timeframe import TimeFrame
# from datetime import datetime, date, timedelta
# import json
# import os
# import sys
# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# import pandas as pd
# import logging
# import time
# import pytz
# from threading import Timer
# import psycopg2

# # === CONFIGURATION === 
# # Alpaca API credentials - MUST be set as environment variables
# API_KEY_ALPACA = os.getenv("API_KEY_ALPACA") 
# API_SECRET_ALPACA = os.getenv("API_SECRET_ALPACA")

# # Kafka broker configuration - MUST be set as an environment variable
# KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# KAFKA_TOPIC = "h_alpaca" # Kafka topic for historical data

# # Database connection details for fetching tickers - MUST be set as environment variables
# POSTGRES_HOST = os.getenv("POSTGRES_HOST")
# POSTGRES_PORT = os.getenv("POSTGRES_PORT")
# POSTGRES_DB = os.getenv("POSTGRES_DB")
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# # Date range for historical data download
# START_DATE = datetime(2021, 1, 1)
# END_DATE = datetime.combine(datetime.today().date() - timedelta(days=1), datetime.min.time())

# # Checkpoint file to track processed data
# CHECKPOINT_FILE = "checkpoint.json"

# # === LOGGING SETUP ===
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# # === Custom JSON Encoder for datetime objects ===
# class DateTimeEncoder(json.JSONEncoder):
#     """Custom JSON encoder to handle datetime, date, and pandas Timestamp objects."""
#     def default(self, obj):
#         if isinstance(obj, (datetime, date, pd.Timestamp)):
#             return obj.isoformat()
#         return super().default(obj)

# # === Kafka Connection Function ===
# def connect_kafka():
#     """Establishes and returns a KafkaProducer connection, with retry logic."""
#     # Validate Kafka broker config
#     if not KAFKA_BOOTSTRAP_SERVERS:
#         logger.critical("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
#         sys.exit(1)

#     while True:
#         try:
#             producer = KafkaProducer(
#                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                 value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8'),
#                 key_serializer=lambda k: k.encode('utf-8') if k else None,
#                 acks='all', # Ensure all replicas have acknowledged the message
#                 retries=3,  # Retry failed message sends
#                 batch_size=16384, # Buffer messages to send in batches of 16KB
#                 linger_ms=10, # Wait up to 10ms to fill a batch before sending
#                 buffer_memory=33554432 # Total buffer memory for unsent records
#             )
#             logger.info("Successfully connected to Kafka.")
#             return producer
#         except Exception as e:
#             logger.warning(f"Kafka not available, retrying in 5 seconds... ({e})")
#             time.sleep(5)

# # === Checkpoint Functions ===
# def load_checkpoint():
#     """Loads the checkpoint file to resume processing. Returns an empty dict if not found or on error."""
#     if os.path.exists(CHECKPOINT_FILE):
#         try:
#             with open(CHECKPOINT_FILE, "r") as f:
#                 return json.load(f)
#         except Exception as e:
#             logger.warning(f"Could not read checkpoint file: {e}. Starting fresh.")
#     return {}

# def save_checkpoint(cp):
#     """Saves the current checkpoint state to a file."""
#     try:
#         with open(CHECKPOINT_FILE, "w") as f:
#             json.dump(cp, f, indent=2)
#     except Exception as e:
#         logger.error(f"Error writing checkpoint file: {e}")

# # === Database Ticker Fetching Function ===
# def fetch_tickers_from_db():
#     """
#     Fetches active stock tickers from the PostgreSQL database.
#     Includes retry logic for database connection.
#     Exits if persistent connection failure occurs.
#     """
#     # Validate database config
#     if not all([POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
#         logger.critical("One or more PostgreSQL environment variables (HOST, PORT, DB, USER, PASSWORD) are not set. Exiting.")
#         sys.exit(1)

#     max_retries = 15 # Increased retries as DB might take longer to start
#     initial_delay = 5
#     for i in range(max_retries):
#         try:
#             conn = psycopg2.connect(
#                 host=POSTGRES_HOST,
#                 port=POSTGRES_PORT,
#                 database=POSTGRES_DB,
#                 user=POSTGRES_USER,
#                 password=POSTGRES_PASSWORD
#             )
#             cursor = conn.cursor()
            
#             # Attempt to select active tickers; fallback if 'is_active' column doesn't exist
#             try:
#                 cursor.execute("SELECT DISTINCT ticker FROM companies_info WHERE is_active = TRUE;")
#             except psycopg2.ProgrammingError:
#                 logger.warning("Column 'is_active' not found in 'companies_info'. Falling back to fetching all distinct tickers.")
#                 cursor.execute("SELECT DISTINCT ticker FROM companies_info;")
            
#             result = cursor.fetchall()
#             tickers = [row[0] for row in result if row[0] is not None]
#             cursor.close()
#             conn.close()
            
#             if not tickers:
#                 logger.warning("No active tickers found in the database. Ensure 'companies_info' table is populated and 'is_active' flags are set, or the table exists.")
#             else:
#                 logger.info(f"Fetched {len(tickers)} active tickers from DB.")
#             return tickers
#         except psycopg2.OperationalError as e:
#             logger.warning(f"Database not available, retrying in {initial_delay * (i + 1)} seconds... ({e})")
#             time.sleep(initial_delay * (i + 1)) # Exponential backoff
#         except Exception as e:
#             logger.error(f"Error fetching tickers from database: {e}")
#             # Do not exit immediately on general error, allow retry
#             time.sleep(initial_delay * (i + 1)) # Wait before next retry
#     logger.critical(f"Failed to connect to database after {max_retries} attempts. Exiting.")
#     sys.exit(1) # Exit if database connection fails persistently

# # Fetch tickers from the database on script start
# TICKERS_FROM_DB = fetch_tickers_from_db()

# # Exit if no tickers are found from the database, as the script cannot proceed meaningfully
# if not TICKERS_FROM_DB:
#     logger.critical("No tickers available from the database. Exiting application.")
#     sys.exit(1)

# # === Download Data for a Ticker Function ===
# def download_ticker_data(ticker):
#     """Downloads historical bar data for a given ticker from Alpaca."""
#     # Validate Alpaca API credentials
#     if not API_KEY_ALPACA or not API_SECRET_ALPACA:
#         logger.critical("Alpaca API_KEY_ALPACA or API_SECRET_ALPACA environment variables are not set. Exiting.")
#         sys.exit(1)

#     logger.info(f"Downloading data for {ticker}...")

#     client = StockHistoricalDataClient(API_KEY_ALPACA, API_SECRET_ALPACA)
#     req = StockBarsRequest(
#         symbol_or_symbols=ticker,
#         timeframe=TimeFrame.Minute,
#         start=START_DATE,
#         end=END_DATE
#     )

#     try:
#         bars = client.get_stock_bars(req).df
#         if bars.empty:
#             logger.warning(f"No data available for {ticker} in the specified range.")
#             return None

#         # Process and filter data for market hours
#         df = bars.reset_index()
#         df = df.set_index('timestamp')
#         df = df.tz_convert("America/New_York") # Convert to NY timezone for market hours filtering
#         df = df.between_time("09:30", "16:00") # Filter for market hours (9:30 AM to 4:00 PM ET)
#         df = df.tz_convert("UTC").reset_index() # Convert back to UTC for consistency
#         df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True) # Ensure timestamp is proper datetime object
#         logger.info(f"Downloaded {len(df)} filtered records for {ticker}")
#         return df

#     except Exception as e:
#         logger.error(f"Error downloading data for {ticker}: {e}")
#         return None

# # === Send Data to Kafka Function ===
# def send_data_to_kafka(producer, ticker, df, checkpoint):
#     """Sends historical data to Kafka, grouped by day, and updates checkpoint."""
#     if df is None or df.empty:
#         return

#     df['date'] = df['timestamp'].dt.date
#     grouped = df.groupby('date')

#     for date_val, group_df in grouped:
#         date_str = str(date_val)
#         if date_str in checkpoint.get(ticker, []):
#             logger.info(f"Day {ticker} {date_str} already processed, skipping.")
#             continue

#         message = {
#             'ticker': ticker,
#             'date': date_str,
#             'year': date_val.year,
#             'month': date_val.month,
#             'data': group_df.drop('date', axis=1).to_dict('records') # Convert DataFrame group to list of dicts
#         }

#         try:
#             # Using ticker_date_str as key for partitioning and ordering
#             future = producer.send(
#                 topic=KAFKA_TOPIC,
#                 key=f"{ticker}_{date_str}",
#                 value=message
#             )
#             future.get(timeout=10) # Block until message is sent (or timeout) to ensure delivery
#             logger.info(f"Sent {len(group_df)} records for {ticker} on {date_str}")
            
#             # Update checkpoint and save after successful send
#             checkpoint.setdefault(ticker, []).append(date_str)
#             save_checkpoint(checkpoint)
#             reset_timer() # Reset inactivity timer on successful data processing

#         except KafkaError as e:
#             logger.error(f"KafkaError sending data for {ticker} on {date_str}: {e}")
#         except Exception as e:
#             logger.error(f"Generic error sending data for {ticker} on {date_str}: {e}")


# # === Inactivity Timer Functions ===
# inactivity_timer = None
# TIMEOUT_SECONDS = 300 # 5 minutes of inactivity before exiting

# def timeout_exit():
#     """Function called if no data is processed for a certain period."""
#     logger.warning(f"No data processed in the last {TIMEOUT_SECONDS} seconds. Shutting down producer.")
#     os._exit(0) # Force exit if stuck

# def reset_timer():
#     """Resets or starts the inactivity timer."""
#     global inactivity_timer
#     if inactivity_timer:
#         inactivity_timer.cancel() # Cancel existing timer if any
#     inactivity_timer = Timer(TIMEOUT_SECONDS, timeout_exit) # Start a new timer
#     inactivity_timer.start()

# # === Main Execution Function ===
# def main():
#     """Main function to orchestrate historical data download and Kafka ingestion."""
#     global checkpoint
#     logger.info("Starting Alpaca Historical Data Producer -> Kafka pipeline.")
#     producer = connect_kafka() # Establish Kafka connection
#     checkpoint = load_checkpoint() # Load processing checkpoint

#     reset_timer() # Start inactivity timer as the script begins

#     try:
#         processed_tickers_count = 0
#         failed_tickers_count = 0

#         for ticker in TICKERS_FROM_DB: # Iterate over dynamically fetched tickers
#             try:
#                 # Get all business days within the specified date range
#                 expected_dates = pd.date_range(START_DATE.date(), END_DATE.date(), freq='B').strftime("%Y-%m-%d").tolist()
#                 done_dates = set(checkpoint.get(ticker, []))
                
#                 # If all expected dates for this ticker are already processed, skip
#                 if all(d in done_dates for d in expected_dates):
#                     logger.info(f"Ticker {ticker} already fully processed, skipping.")
#                     processed_tickers_count += 1
#                     continue

#                 # Download data for the ticker
#                 df = download_ticker_data(ticker)
#                 if df is not None:
#                     # Send downloaded data to Kafka
#                     send_data_to_kafka(producer, ticker, df, checkpoint)
#                     processed_tickers_count += 1
#                 else:
#                     failed_tickers_count += 1

#             except Exception as e:
#                 logger.error(f"Error processing ticker {ticker}: {e}", exc_info=True) # Log full traceback
#                 failed_tickers_count += 1

#         producer.flush() # Ensure all buffered messages are sent before concluding
#         logger.info(f"Processing complete! Successful tickers: {processed_tickers_count}, Errors/Skipped: {failed_tickers_count}")

#     except KeyboardInterrupt:
#         logger.info("User interruption detected. Shutting down gracefully.")
#     except Exception as e:
#         logger.critical(f"Fatal error occurred during main execution: {e}", exc_info=True)
#     finally:
#         # Ensure inactivity timer is cancelled before final shutdown
#         if inactivity_timer:
#             inactivity_timer.cancel()
        
#         save_checkpoint(checkpoint) # Always save final checkpoint
#         if producer:
#             logger.info("Closing Kafka producer connection.")
#             producer.close() # Close Kafka producer
#         logger.info("Producer terminated.")

# if __name__ == "__main__":
#     main()




























































from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, date, timedelta
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
KAFKA_TOPIC = "h_alpaca" # Topic per i dati raw di Alpaca

# Database connection details for fetching tickers - MUST be set as environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Date range for historical data download
START_DATE = datetime(2021, 1, 1) # Starting from 2021 for broader historical data
END_DATE = datetime.combine(datetime.today().date() - timedelta(days=1), datetime.min.time())

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
    if not KAFKA_BOOTSTRAP_SERVERS:
        logger.critical("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
        sys.exit(1)

    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
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
    if not all([POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
        logger.critical("One or more PostgreSQL environment variables (HOST, PORT, DB, USER, PASSWORD) are not set. Exiting.")
        sys.exit(1)

    max_retries = 15
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
            time.sleep(initial_delay * (i + 1))
        except Exception as e:
            logger.error(f"Error fetching tickers from database: {e}")
            time.sleep(initial_delay * (i + 1))
    logger.critical(f"Failed to connect to database after {max_retries} attempts. Exiting.")
    sys.exit(1)

# Fetch tickers from the database on script start
TICKERS_FROM_DB = fetch_tickers_from_db()

if not TICKERS_FROM_DB:
    logger.critical("No tickers available from the database. Exiting application.")
    sys.exit(1)

# === Download Data for a Ticker Function ===
def download_ticker_data(ticker):
    """Downloads historical bar data for a given ticker from Alpaca."""
    if not API_KEY_ALPACA or not API_SECRET_ALPACA:
        logger.critical("Alpaca API_KEY_ALPACA or API_SECRET_ALPACA environment variables are not set. Exiting.")
        sys.exit(1)

    logger.info(f"Downloading data for {ticker} from {START_DATE.date()} to {END_DATE.date()}...")

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

        df = bars.reset_index()
        df = df.set_index('timestamp')
        df = df.tz_convert("America/New_York")
        df = df.between_time("09:30", "16:00")
        df = df.tz_convert("UTC").reset_index()
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        if 'symbol' not in df.columns:
            df['symbol'] = ticker
        
        df.columns = df.columns.str.lower()

        logger.info(f"Downloaded {len(df)} filtered records for {ticker}")
        return df

    except Exception as e:
        logger.error(f"Error downloading data for {ticker}: {e}")
        return None

# === Send Weekly Data to Kafka Function ===
def send_weekly_data_to_kafka(producer, ticker, df_full, checkpoint):
    """
    Splits the full DataFrame into weekly chunks and sends each chunk
    as a separate Kafka message, updating checkpoint.
    Returns the number of unique weekly chunks sent in this call.
    """
    if df_full is None or df_full.empty:
        logger.warning(f"No full data DataFrame for {ticker}. Skipping weekly chunk processing for this ticker.")
        return 0, 0 # returns (sent_count, skipped_count)

    sent_count = 0
    skipped_count = 0
    
    # Generate unique (year, week_of_year) combinations and sort them
    unique_year_weeks = df_full['timestamp'].apply(lambda x: (x.isocalendar().year, x.isocalendar().week)).unique()
    sorted_year_weeks = sorted(list(unique_year_weeks))

    # --- INIZIO MODIFICA MAGGIORE ---
    # Per assicurarti che vengano generate tutte le settimane teoriche nell'intervallo del Producer,
    # puoi generare l'elenco delle settimane qui, piuttosto che basarti solo su `df_full`.
    # Questo allinea il Producer al conteggio 'teorico' dell'Aggregator.

    # Calcola tutte le settimane ISO teoriche tra START_DATE e END_DATE
    # Utilizziamo la stessa logica di allineamento a Lunedì-Domenica della funzione count_weeks nell'aggregatore.
    start_date_iso = START_DATE.date() # Converti a date per coerenza
    end_date_iso = END_DATE.date()     # Converti a date per coerenza

    # Allinea start_date al Lunedì della sua settimana ISO
    start_monday_iso = start_date_iso - timedelta(days=start_date_iso.weekday())
    
    # Allinea end_date alla Domenica della sua settimana ISO
    end_sunday_iso = end_date_iso + timedelta(days=(6 - end_date_iso.weekday()) % 7)

    all_theoretical_iso_weeks = set()
    current_week_start = start_monday_iso
    while current_week_start <= end_sunday_iso:
        # isocalendar() della data di inizio settimana è il modo più affidabile
        year, week_num, _ = current_week_start.isocalendar()
        all_theoretical_iso_weeks.add((year, week_num))
        current_week_start += timedelta(weeks=1)
    
    sorted_theoretical_iso_weeks = sorted(list(all_theoretical_iso_weeks))
    
    # Itera su *tutte le settimane teoriche*, non solo quelle che hanno dati nel df_full
    for year, week_num in sorted_theoretical_iso_weeks:
    # --- FINE MODIFICA MAGGIORE ---

        checkpoint_key = f"{ticker}_{year}_{week_num}"
        if checkpoint_key in checkpoint.get("processed_weeks", []):
            logger.info(f"Ticker {ticker} week {year}-{week_num} already processed, skipping send.")
            skipped_count += 1
            continue

        # Estrai i dati solo per la settimana corrente dal DataFrame completo.
        # Se non ci sono dati nel df_full per questa settimana, df_week sarà vuoto.
        df_week = df_full[(df_full['timestamp'].dt.isocalendar().year == year) & 
                           (df_full['timestamp'].dt.isocalendar().week == week_num)].copy()
        
        message_data = df_week.to_dict('records') # Sarà una lista vuota se df_week è vuoto

        message = {
            'ticker': ticker,
            'year': year,
            'week': week_num,
            'data': message_data # Questa sarà vuota [] se non ci sono dati per la settimana
        }

        try:
            kafka_key = f"{ticker}_{year}_{week_num}"
            future = producer.send(
                topic=KAFKA_TOPIC,
                key=kafka_key,
                value=message
            )
            future.get(timeout=30)
            
            if not message_data: # Se il batch era vuoto
                logger.info(f"Sent EMPTY batch for {ticker} week {year}-{week_num} (no data found).")
            else:
                logger.info(f"Sent {len(message_data)} records for {ticker} week {year}-{week_num}.")
            
            checkpoint.setdefault("processed_weeks", []).append(checkpoint_key)
            save_checkpoint(checkpoint) # Salva il checkpoint indipendentemente dal fatto che i dati siano vuoti o meno
            reset_timer()
            sent_count += 1 # Conta come inviato, anche se vuoto

        except KafkaError as e:
            logger.error(f"KafkaError sending data for {ticker} week {year}-{week_num}: {e}")
        except Exception as e:
            logger.error(f"Generic error sending data for {ticker} week {year}-{week_num}: {e}")
            
    return sent_count, skipped_count

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
        inactivity_timer.cancel()
    inactivity_timer = Timer(TIMEOUT_SECONDS, timeout_exit)
    inactivity_timer.start()

# === Main Execution Function ===
def main():
    """Main function to orchestrate historical data download and Kafka ingestion."""
    global checkpoint
    logger.info("Starting Alpaca Historical Data Producer -> Kafka pipeline.")
    producer = connect_kafka()
    checkpoint = load_checkpoint()

    if "processed_weeks" not in checkpoint:
        checkpoint["processed_weeks"] = []

    reset_timer()

    try:
        total_weeks_sent_in_this_run = 0 # Count of distinct week-chunks *sent* in this execution
        total_weeks_skipped_in_this_run = 0 # Count of distinct week-chunks *skipped* in this execution (already in checkpoint or no data)
        failed_tickers_download = 0 # Count of tickers for which download failed completely
        
        # Calculate all expected ticker-week combinations for the full range
        all_expected_ticker_weeks = set()
        current_date_iter = START_DATE.date()
        while current_date_iter <= END_DATE.date():
            for ticker in TICKERS_FROM_DB:
                year = current_date_iter.isocalendar().year
                week_num = current_date_iter.isocalendar().week
                all_expected_ticker_weeks.add(f"{ticker}_{year}_{week_num}")
            current_date_iter += timedelta(days=1)
        
        # Determine which tickers *still need* processing (have at least one unprocessed week)
        tickers_to_download = []
        for ticker in TICKERS_FROM_DB:
            needs_download = False
            temp_date = START_DATE.date()
            while temp_date <= END_DATE.date():
                chk_key = f"{ticker}_{temp_date.isocalendar().year}_{temp_date.isocalendar().week}"
                if chk_key not in checkpoint["processed_weeks"]:
                    needs_download = True
                    break # Found at least one unprocessed week, so this ticker needs attention
                temp_date += timedelta(days=1)
            
            if needs_download:
                tickers_to_download.append(ticker)
            else:
                logger.info(f"Ticker {ticker} appears fully processed based on checkpoint, skipping download.")
        
        # Process tickers that need attention
        for ticker in tickers_to_download:
            logger.info(f"Processing ticker: {ticker}")
            try:
                df = download_ticker_data(ticker)
                if df is not None and not df.empty:
                    sent, skipped = send_weekly_data_to_kafka(producer, ticker, df, checkpoint)
                    total_weeks_sent_in_this_run += sent
                    total_weeks_skipped_in_this_run += skipped
                else:
                    logger.warning(f"No data or empty DataFrame for ticker {ticker}. Could not process weekly chunks.")
                    failed_tickers_download += 1

            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {e}", exc_info=True)
                failed_tickers_download += 1
        
        producer.flush(timeout=60)
        
        # Final check if ALL expected ticker-week combinations are in the checkpoint
        # This is the definitive check for "FINITO!"
        all_expected_in_checkpoint = True
        for expected_key in all_expected_ticker_weeks:
            if expected_key not in checkpoint["processed_weeks"]:
                all_expected_in_checkpoint = False
                break
        
        if all_expected_in_checkpoint:
            logger.info(f"All expected ticker-week combinations ({len(all_expected_ticker_weeks)}) are now processed in checkpoint.")
            print("FINITO!")
        else:
            logger.warning(f"NOT ALL expected ticker-week combinations ({len(all_expected_ticker_weeks)}) are processed in checkpoint. Missing {len(all_expected_ticker_weeks) - len(checkpoint['processed_weeks'])} chunks. Check logs for details.")

        logger.info(f"Producer finished run. Weekly chunks sent in this run: {total_weeks_sent_in_this_run}.")
        logger.info(f"Weekly chunks skipped (already in checkpoint or no data for week): {total_weeks_skipped_in_this_run}.")
        logger.info(f"Tickers with download errors or no data: {failed_tickers_download}.")
        logger.info(f"Total unique weekly chunks now recorded in checkpoint: {len(checkpoint['processed_weeks'])} out of {len(all_expected_ticker_weeks)} expected.")

    except KeyboardInterrupt:
        logger.info("User interruption detected. Shutting down gracefully.")
    except Exception as e:
        logger.critical(f"Fatal error occurred during main execution: {e}", exc_info=True)
    finally:
        if inactivity_timer:
            inactivity_timer.cancel()
        
        save_checkpoint(checkpoint)
        if producer:
            logger.info("Closing Kafka producer connection.")
            producer.close()
        logger.info("Producer terminated.")

if __name__ == "__main__":
    main()