from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
import json
import numpy as np
import pandas as pd
import os
import sys
import psycopg2
from psycopg2 import sql
from psycopg2 import sql, OperationalError
import threading
import time
from kafka.admin import KafkaAdminClient, NewTopic
import kafka
from kafka import KafkaProducer

# ==== Global Configurations ====
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPICS = ["h_alpaca", "h_macrodata", "h_company"]

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
DB_TABLE_NAME = "aggregated_data"

# ==== Date Range Configuration ====
START_DATE = datetime(2021, 1, 1)
END_DATE = datetime.combine(datetime.today().date() - timedelta(days=1), datetime.min.time())

# ==== Helper to calculate number of ISO weeks in a date range ====
def count_weeks(start_date_obj, end_date_obj):
    """
    Counts the number of unique ISO weeks within a date range.
    start_date_obj and end_date_obj are datetime objects.
    """
    start_date_iso = start_date_obj.date()
    end_date_iso = end_date_obj.date()

    # Find the Monday of the start week
    start_monday = start_date_iso - timedelta(days=start_date_iso.weekday())

    # Find the Sunday of the end week
    end_sunday = end_date_iso + timedelta(days=(6 - end_date_iso.weekday()) % 7)

    num_weeks = (end_sunday - start_monday).days // 7 + 1
    return num_weeks

# ==== Database Connection with Retries for Initial Ticker Load ====
def connect_to_db_for_tickers(max_retries=15, delay=5):
    """Attempts to connect to the database with retry logic."""
    db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(db_url)
            sys.stderr.write(f"INFO: Connected to PostgreSQL successfully for tickers (attempt {i+1}).\n")
            return conn
        except OperationalError as e:
            sys.stderr.write(f"ERROR: PostgreSQL connection failed for tickers: {e}. Retrying in {delay}s...\n")
        except Exception as e:
            sys.stderr.write(f"ERROR: Unexpected error during DB connection for tickers: {e}. Retrying in {delay}s...\n")
        time.sleep(delay)
    sys.stderr.write("CRITICAL ERROR: Max retries reached. Could not connect to PostgreSQL for tickers. Exiting.\n")
    raise Exception("Failed to connect to database for tickers.")

# ==== Function to Load TICKERS from Database ====
def load_tickers_from_db():
    """
    Loads the list of active tickers from the 'companies_info' table in the database.
    """
    tickers_list = []
    conn = None
    try:
        conn = connect_to_db_for_tickers()
        cursor = conn.cursor()
        cursor.execute(
            sql.SQL("SELECT ticker FROM companies_info WHERE is_active = TRUE ORDER BY ticker_id")
        )
        rows = cursor.fetchall()
        cursor.close()
        
        tickers_list = [row[0] for row in rows]
        sys.stderr.write(f"INFO: Loaded {len(tickers_list)} active tickers from DB.\n")
        return tickers_list
    except Exception as e:
        sys.stderr.write(f"CRITICAL ERROR: Failed to load TICKERS from database: {e}\n")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

# ==== Global Ticker List (Dynamically Loaded) ====
TICKERS = load_tickers_from_db()

# ==== Global Data Stores (Shared across process_element calls, but ensure Flink's design handles state correctly) ====
macro_data_by_series = {}
fundamentals_by_symbol_year = {}
pending_stock_batches = []
ready_flags = {"macro_ready": False, "fundamentals_ready": False}

# ==== Helper Functions (Independent of Flink's RuntimeContext) ====
def get_macro_values_for_date(date_obj):
    """Retrieves the latest available macro data for a given date."""
    result = {}
    for series, entries in macro_data_by_series.items():
        selected_value = None
        # Entries are assumed to be sorted by date
        for entry_date, value in entries:
            if entry_date <= date_obj:
                selected_value = value
            else:
                break
        if selected_value is not None:
            result[series] = selected_value
    return result

def get_fundamentals(symbol, year):
    """Retrieves fundamental data for a given symbol and year."""
    return fundamentals_by_symbol_year.get((symbol, year), {})

# ==== Flink KeyedProcessFunction for Aggregation ====
class FullDayAggregator(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        """
        Initializes the database connection. Called once per parallel instance of the function.
        """
        self.db_name = POSTGRES_DB
        self.db_user = POSTGRES_USER
        self.db_password = POSTGRES_PASSWORD
        self.db_host = POSTGRES_HOST
        self.db_port = POSTGRES_PORT
        self.table_name = DB_TABLE_NAME

        self.conn = None
        self.cursor = None
        self._connect_to_db()
        
        self.kafka_producer_properties = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
        }

        # Initialize batch counter for this instance
        self.processed_batches = 0
        num_weeks = count_weeks(START_DATE, END_DATE)
        num_tickers = len(TICKERS)
        self.total_expected_batches = num_tickers * num_weeks
        print(f"[INFO] Expecting {self.total_expected_batches} total batches ({num_tickers} tickers × {num_weeks} weeks)")

    def _connect_to_db(self):
        """Establishes a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            self.cursor = self.conn.cursor()
            print("[INFO] Successfully connected to PostgreSQL.")
            self._create_table_if_not_exists()
        except Exception as e:
            print(f"[ERROR] Could not connect to PostgreSQL: {e}", file=sys.stderr)
            raise RuntimeError(f"Failed to connect to PostgreSQL: {e}")

    def _create_table_if_not_exists(self):
        """Creates the aggregated_data table if it doesn't already exist."""
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ticker VARCHAR(10) NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                price_mean_1min DOUBLE PRECISION,
                price_mean_5min DOUBLE PRECISION,
                price_std_5min DOUBLE PRECISION,
                price_mean_30min DOUBLE PRECISION,
                price_std_30min DOUBLE PRECISION,
                size_tot_1min DOUBLE PRECISION,
                size_tot_5min DOUBLE PRECISION,
                size_tot_30min DOUBLE PRECISION,
                sentiment_bluesky_mean_2hours DOUBLE PRECISION,
                sentiment_bluesky_mean_1day DOUBLE PRECISION,
                sentiment_news_mean_1day DOUBLE PRECISION,
                sentiment_news_mean_3days DOUBLE PRECISION,
                sentiment_general_bluesky_mean_2hours DOUBLE PRECISION,
                sentiment_general_bluesky_mean_1day DOUBLE PRECISION,
                minutes_since_open DOUBLE PRECISION,
                day_of_week INTEGER,
                day_of_month INTEGER,
                week_of_year INTEGER,
                month_of_year INTEGER,
                market_open_spike_flag INTEGER,
                market_close_spike_flag INTEGER,
                eps DOUBLE PRECISION,
                free_cash_flow DOUBLE PRECISION,
                profit_margin DOUBLE PRECISION,
                debt_to_equity DOUBLE PRECISION,
                gdp_real DOUBLE PRECISION,
                cpi DOUBLE PRECISION,
                ffr DOUBLE PRECISION,
                t10y DOUBLE PRECISION,
                t2y DOUBLE PRECISION,
                spread_10y_2y DOUBLE PRECISION,
                unemployment DOUBLE PRECISION,
                y1 DOUBLE PRECISION,
                PRIMARY KEY (ticker, timestamp)
            );
        """).format(table_name=sql.Identifier(self.table_name))
        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            print(f"[INFO] Table '{self.table_name}' checked/created successfully.")
        except Exception as e:
            print(f"[ERROR] Could not create table {self.table_name}: {e}", file=sys.stderr)
            self.conn.rollback()
            raise

    def close(self):
        """Closes the database connection. Called when the Flink task is closing."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("[INFO] PostgreSQL connection closed.")

    def process_element(self, value: str, ctx):
        """
        Processes each incoming element from the Kafka stream.
        Handles macro, fundamental, and stock data.
        """
        try:
            message = json.loads(value)

            # ==== Process Macro Data ====
            if "series" in message and "value" in message and "date" in message:
                try:
                    series = message["series"]
                    date = datetime.fromisoformat(message["date"]).date()
                    val = float(message["value"])
                    macro_data_by_series.setdefault(series, []).append((date, val))
                    macro_data_by_series[series].sort() # Keep sorted for get_macro_values_for_date

                    required_macro_series = ["gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"]
                    if all(s in macro_data_by_series and macro_data_by_series[s] for s in required_macro_series):
                        ready_flags["macro_ready"] = True
                        print("[INFO] Macro data ready.")

                except Exception as e:
                    print(f"[ERROR] Error processing macro data: {e} - Message: {message}", file=sys.stderr)
                return

            # ==== Process Fundamental Data ====
            if "symbol" in message and "calendarYear" in message:
                try:
                    symbol = message["symbol"]
                    year = int(message["calendarYear"])
                    fundamentals_by_symbol_year[(symbol, year)] = {
                        "eps": message.get("eps"),
                        "freeCashFlow": message.get("cashflow_freeCashFlow"),
                        "revenue": message.get("revenue"),
                        "netIncome": message.get("netIncome"),
                        "balance_totalDebt": message.get("balance_totalDebt"),
                        "balance_totalStockholdersEquity": message.get("balance_totalStockholdersEquity")
                    }
                    ready_flags["fundamentals_ready"] = True
                    print("[INFO] Fundamental data ready.")
                except Exception as e:
                    print(f"[ERROR] Error processing fundamental data: {e} - Message: {message}", file=sys.stderr)
                return

            # ==== Process Buffered Stock Batches if Ready ====
            if ready_flags["macro_ready"] and ready_flags["fundamentals_ready"] and pending_stock_batches:
                print(f"[INFO] Processing {len(pending_stock_batches)} buffered stock batches.", file=sys.stdout)
                for buffered_value in list(pending_stock_batches):
                    try:
                        self._process_stock_message(json.loads(buffered_value))
                    except Exception as e:
                        print(f"[ERROR] Error processing buffered stock message: {e}", file=sys.stderr)
                pending_stock_batches.clear()

            # ==== Buffer Stock Data if not Ready ====
            if not (ready_flags["macro_ready"] and ready_flags["fundamentals_ready"]):
                if "ticker" in message and "data" in message:
                    pending_stock_batches.append(value)
                   
                return

            # ==== Process Live Stock Data (if all ready and not buffered) ====
            if "ticker" in message and "data" in message:
                self._process_stock_message(message)

        except json.JSONDecodeError:
            print(f"[ERROR] JSON decoding failed for message: {value[:200]}...", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] Global error in process_element: {e} - Original Value: {value[:200]}...", file=sys.stderr)


    def _process_stock_message(self, message):
            """
            Processes a single stock data message, calculates features, and inserts into PostgreSQL.
            """
            ticker = message.get("ticker")
            if ticker not in TICKERS:
                
                return

            # Increment batch counter
            self.processed_batches += 1
            print(f"[INFO] Processed batch {self.processed_batches}/{self.total_expected_batches} for ticker {ticker}")

            if self.processed_batches >= self.total_expected_batches:
                print(f"[INFO] All {self.total_expected_batches} batches processed. Shutting down...")
                self._send_model_start_signal()
                return 

            rows = message.get("data", [])
            parsed = []
            for entry in rows:
                try:
                    parsed.append({
                        "symbol": ticker,
                        "timestamp": datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00")),
                        "open": float(entry["open"]),
                        "close": float(entry["close"]),
                        "volume": float(entry["volume"]),
                    })
                except Exception as e:
                    print(f"[ERROR] Error parsing stock entry for {ticker}: {e} - Entry: {entry}", file=sys.stderr)
                    continue

            if not parsed:
                return
            parsed.sort(key=lambda x: x["timestamp"])
            batch_results = []

            for i in range(len(parsed)):
                row = parsed[i]
                now = row["timestamp"]

                def window_vals(field, minutes):
                    ts_limit = now - timedelta(minutes=minutes)
                    return [p[field] for p in parsed if ts_limit <= p["timestamp"] <= now]

                def mean(values): return float(np.mean(values)) if values else None
                def std(values): return float(np.std(values)) if values else None
                def total(values): return float(np.sum(values)) if values else 0.0

                # Get Macro and Fundamental Data
                macro_values = get_macro_values_for_date(now.date())
                # For fundamentals, using previous year as typical for annual reports
                fundamentals = get_fundamentals(row["symbol"], now.year - 1)

                # Calculate Derived Fundamentals
                profit_margin = None
                if fundamentals.get("revenue") not in [None, 0]:
                    profit_margin = fundamentals["netIncome"] / fundamentals["revenue"]

                debt_to_equity = None
                if fundamentals.get("balance_totalStockholdersEquity") not in [None, 0]:
                    debt_to_equity = fundamentals["balance_totalDebt"] / fundamentals["balance_totalStockholdersEquity"]

                # Time-based Features (using New York time for market hours)
                local_time = now.astimezone(ZoneInfo("America/New_York"))
                market_open_flag = int(dtime(9, 30) <= local_time.time() <= dtime(9, 34))
                market_close_flag = int(dtime(15, 56) <= local_time.time() <= dtime(16, 0))

                minutes_since_open = (local_time - local_time.replace(hour=9, minute=30, second=0, microsecond=0)).total_seconds() // 60
                if minutes_since_open < 0: # If before market open
                    minutes_since_open = 0

                result = {
                    "ticker": row["symbol"],
                    "timestamp": now.isoformat(),
                    "price_mean_1min": mean(window_vals("open", 1)),
                    "price_mean_5min": mean(window_vals("open", 5)),
                    "price_std_5min": std(window_vals("open", 5)),
                    "price_mean_30min": mean(window_vals("open", 30)),
                    "price_std_30min": std(window_vals("open", 30)),
                    "size_tot_1min": total(window_vals("volume", 1)),
                    "size_tot_5min": total(window_vals("volume", 5)),
                    "size_tot_30min": total(window_vals("volume", 30)),
                    # Sentiment values are hardcoded to 0.0, indicating they might come from other sources later
                    "sentiment_bluesky_mean_2hours": 0.0,
                    "sentiment_bluesky_mean_1day": 0.0,
                    "sentiment_news_mean_1day": 0.0,
                    "sentiment_news_mean_3days": 0.0,
                    "sentiment_general_bluesky_mean_2hours": 0.0,
                    "sentiment_general_bluesky_mean_1day": 0.0,
                    "minutes_since_open": minutes_since_open,
                    "day_of_week": now.weekday(),
                    "day_of_month": now.day,
                    "week_of_year": now.isocalendar()[1],
                    "month_of_year": now.month,
                    "market_open_spike_flag": market_open_flag,
                    "market_close_spike_flag": market_close_flag,
                    "eps": fundamentals.get("eps"),
                    "free_cash_flow": fundamentals.get("freeCashFlow"),
                    "profit_margin": profit_margin,
                    "debt_to_equity": debt_to_equity,
                    "gdp_real": macro_values.get("gdp_real", None),
                    "cpi": macro_values.get("cpi", None),
                    "ffr": macro_values.get("ffr", None),
                    "t10y": macro_values.get("t10y", None),
                    "t2y": macro_values.get("t2y", None),
                    "spread_10y_2y": macro_values.get("spread_10y_2y", None),
                    "unemployment": macro_values.get("unemployment", None),
                    "y1": row["close"] # Assuming 'y1' is the target variable, here the closing price
                }
                batch_results.append(result)

            if batch_results:
                self._insert_data_to_postgresql(batch_results)

    def _insert_data_to_postgresql(self, data):
        """Inserts a batch of aggregated data into the PostgreSQL table."""
        columns = [
            "ticker", "timestamp", "price_mean_1min", "price_mean_5min", "price_std_5min",
            "price_mean_30min", "price_std_30min", "size_tot_1min", "size_tot_5min",
            "size_tot_30min", "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
            "sentiment_news_mean_1day", "sentiment_news_mean_3days",
            "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
            "minutes_since_open", "day_of_week", "day_of_month", "week_of_year",
            "month_of_year", "market_open_spike_flag", "market_close_spike_flag",
            "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
            "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
            "y1"
        ]
        
        values_placeholder = sql.SQL(', ').join(sql.Placeholder() * len(columns))
        
        # Use self.table_name here
        insert_query = sql.SQL(
            "INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder}) "
            "ON CONFLICT (ticker, timestamp) DO UPDATE SET "
            "price_mean_1min = EXCLUDED.price_mean_1min, "
            "price_mean_5min = EXCLUDED.price_mean_5min, "
            "price_std_5min = EXCLUDED.price_std_5min, "
            "price_mean_30min = EXCLUDED.price_mean_30min, "
            "price_std_30min = EXCLUDED.price_std_30min, "
            "size_tot_1min = EXCLUDED.size_tot_1min, "
            "size_tot_5min = EXCLUDED.size_tot_5min, "
            "size_tot_30min = EXCLUDED.size_tot_30min, "
            "sentiment_bluesky_mean_2hours = EXCLUDED.sentiment_bluesky_mean_2hours, "
            "sentiment_bluesky_mean_1day = EXCLUDED.sentiment_bluesky_mean_1day, "
            "sentiment_news_mean_1day = EXCLUDED.sentiment_news_mean_1day, "
            "sentiment_news_mean_3days = EXCLUDED.sentiment_news_mean_3days, "
            "sentiment_general_bluesky_mean_2hours = EXCLUDED.sentiment_general_bluesky_mean_2hours, "
            "sentiment_general_bluesky_mean_1day = EXCLUDED.sentiment_general_bluesky_mean_1day, "
            "minutes_since_open = EXCLUDED.minutes_since_open, "
            "day_of_week = EXCLUDED.day_of_week, "
            "day_of_month = EXCLUDED.day_of_month, "
            "week_of_year = EXCLUDED.week_of_year, "
            "month_of_year = EXCLUDED.month_of_year, "
            "market_open_spike_flag = EXCLUDED.market_open_spike_flag, "
            "market_close_spike_flag = EXCLUDED.market_close_spike_flag, "
            "eps = EXCLUDED.eps, "
            "free_cash_flow = EXCLUDED.free_cash_flow, "
            "profit_margin = EXCLUDED.profit_margin, "
            "debt_to_equity = EXCLUDED.debt_to_equity, "
            "gdp_real = EXCLUDED.gdp_real, "
            "cpi = EXCLUDED.cpi, "
            "ffr = EXCLUDED.ffr, "
            "t10y = EXCLUDED.t10y, "
            "t2y = EXCLUDED.t2y, "
            "spread_10y_2y = EXCLUDED.spread_10y_2y, "
            "unemployment = EXCLUDED.unemployment, "
            "y1 = EXCLUDED.y1"
        ).format(
            table_name=sql.Identifier(self.table_name),
            columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values_placeholder=values_placeholder
        )

        try:
            records_to_insert = []
            for record in data:
                record_copy = record.copy()
                if isinstance(record_copy["timestamp"], str):
                    record_copy["timestamp"] = datetime.fromisoformat(record_copy["timestamp"])
                
                row_values = [record_copy.get(col) for col in columns]
                records_to_insert.append(row_values)

            if records_to_insert:
                print(f"[INFO] First row of batch for insertion ({len(records_to_insert)} rows): {records_to_insert[0][0]}, {records_to_insert[0][1]}...", file=sys.stdout)
                self.cursor.executemany(insert_query, records_to_insert)
                self.conn.commit()
                print(f"[INFO] Inserted {len(data)} records into {self.table_name}.", file=sys.stdout)
            else:
                print("[INFO] No records to insert into PostgreSQL for this batch.")
        except Exception as e:
            print(f"[ERROR] An error occurred during PostgreSQL insertion: {e}", file=sys.stderr)
            self.conn.rollback()
            # If the connection breaks, attempt to reconnect
            try:
                print("[INFO] Attempting to reconnect to PostgreSQL...", file=sys.stderr)
                self._connect_to_db()
                print("[INFO] Reconnected to PostgreSQL successfully.", file=sys.stderr)
            except Exception as reconnect_e:
                print(f"[CRITICAL ERROR] Failed to reconnect to PostgreSQL: {reconnect_e}", file=sys.stderr)
                raise

    def _send_model_start_signal(self):
        """Sends a signal to the start_model topic to indicate data processing is complete."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            signal_message = {
                "status": "data_processing_complete",
                "timestamp": datetime.now().isoformat(),
                "total_batches_processed": self.processed_batches,
                "message": "Historical data aggregation completed successfully"
            }
            
            producer.send('start_model', value=signal_message)
            producer.flush()
            producer.close()
            
            print("[INFO] Successfully sent completion signal to start_model topic")
            
        except Exception as e:
            print(f"[ERROR] Failed to send signal to start_model topic: {e}", file=sys.stderr)


# ==== Key Extractor for Flink's key_by operation ====
def extract_key(json_str: str) -> str:
    """
    Extracts a key (ticker, symbol, or series) from the incoming JSON string for Flink's key_by.
    """
    try:
        data = json.loads(json_str)
        # Prioritize ticker for stock data, then symbol for fundamentals, then series for macro
        return data.get("ticker") or data.get("symbol") or data.get("series") or "unknown"
    except json.JSONDecodeError:
        print(f"[ERROR] JSON decoding failed for key extraction: {json_str[:100]}...", file=sys.stderr)
        return "unknown_json_error"
    except Exception as e:
        print(f"[ERROR] Unexpected error in extract_key: {e} - String: {json_str[:100]}...", file=sys.stderr)
        return "unknown_general_error"

def create_kafka_topic_if_not_exists(topic_name, num_partitions=1, replication_factor=1, max_retries=20, retry_delay=10):
    """Crea il topic Kafka se non esiste già."""
    admin_client = None 
    for attempt in range(max_retries):
        try: 
            print(f"[INFO] Tentativo {attempt + 1}/{max_retries} di connessione a Kafka per creare topic '{topic_name}'...")
                
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id='topic_creator'
            )
            
            existing_topics = admin_client.list_topics()
            if topic_name in existing_topics:
                print(f"[INFO] Topic '{topic_name}' già esistente.")
                return
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            
            admin_client.create_topics([topic])
            print(f"[INFO] Topic '{topic_name}' creato con successo.")
            return
        
        except Exception as e:
            print(f"[WARNING] Tentativo {attempt + 1} fallito per topic '{topic_name}': {e}", file=sys.stderr)
            if attempt < max_retries - 1:
                print(f"[INFO] Attendo {retry_delay} secondi prima del prossimo tentativo...")
                time.sleep(retry_delay)
            else:
                print(f"[ERROR] Tutti i {max_retries} tentativi falliti per creare topic '{topic_name}'. Continuo senza creazione topic.", file=sys.stderr)
        finally:
            if admin_client is not None:  # Only close if it was successfully created
                try:
                    admin_client.close()
                    admin_client = None 
                except Exception as close_e:
                    print(f"[WARNING] Error closing admin client: {close_e}", file=sys.stderr)

# ==== Main Flink Job Execution ====
def main():
    """
    Sets up and executes the Flink streaming job.
    """
    global KAFKA_TOPICS, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, DB_TABLE_NAME
    KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
    # ==== Flink Environment Setup ====
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Keep parallelism at 1 if using global dictionaries for macro/fundamentals

    create_kafka_topic_if_not_exists('start_model')

    # ==== Kafka Consumer Setup ====
    consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPICS,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "flink_batch_group",
            "auto.offset.reset": "earliest"
        }
    )

    # ==== Stream Processing Pipeline ====
    stream = env.add_source(consumer, type_info=Types.STRING())
    # Key by extracted key to ensure related data (e.g., same ticker) goes to the same Flink task
    keyed = stream.key_by(extract_key, key_type=Types.STRING())
    # Apply the custom KeyedProcessFunction for aggregation and DB insertion
    processed = keyed.process(FullDayAggregator(), output_type=Types.ROW([])) # Output type can be void if no downstream needed

    # ==== Execute the Flink Job ====
    try:
        env.execute("Historical Aggregation to PostgreSQL with Macro Data")
    except Exception as e:
        if "Job completed successfully" in str(e):
            print("[INFO] Flink job completed successfully - all expected batches processed.")
            sys.exit(0)
        else:
            raise

if __name__ == "__main__":
    main()