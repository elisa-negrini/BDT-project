# import os
# import sys
# import json
# import numpy as np
# from datetime import datetime, timezone, timedelta
# from dateutil.parser import isoparse
# import pytz
# import pandas as pd
# import io

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from minio import Minio
# from minio.error import S3Error
# import psycopg2
# import time


# # Database connection details for fetching tickers - MUST be set as environment variables
# POSTGRES_HOST = os.getenv("POSTGRES_HOST")
# POSTGRES_PORT = os.getenv("POSTGRES_PORT")
# POSTGRES_DB = os.getenv("POSTGRES_DB")
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# def fetch_tickers_from_db():
#     max_retries = 50
#     delay = 5

#     for attempt in range(max_retries):
#         try:
#             conn = psycopg2.connect(
#                 host=POSTGRES_HOST,
#                 port=POSTGRES_PORT,
#                 database=POSTGRES_DB,
#                 user=POSTGRES_USER,
#                 password=POSTGRES_PASSWORD
#             )
#             cursor = conn.cursor()
#             try:
#                 cursor.execute("SELECT DISTINCT ticker FROM companies_info WHERE is_active = TRUE;")
#             except psycopg2.ProgrammingError:
#                 print("Column 'is_active' not found. Falling back to all distinct tickers.")
#                 cursor.execute("SELECT DISTINCT ticker FROM companies_info;")

#             result = cursor.fetchall()
#             tickers = [row[0] for row in result if row[0]]
#             cursor.close()
#             conn.close()

#             if not tickers:
#                 print("No tickers found in the database.")
#             else:
#                 print(f"Loaded {len(tickers)} tickers from DB.")
#             return tickers
#         except Exception as e:
#             print(f"Database not available, retrying in {delay * (attempt + 1)} seconds... ({e})")
#             time.sleep(delay * (attempt + 1))

#     print("Failed to connect to database after multiple attempts. Exiting.")
#     exit(1)

# TOP_TICKERS = fetch_tickers_from_db()
# if not TOP_TICKERS:
#     print("No tickers available from DB. Exiting.")
#     exit(1)


# RETRY_DELAY_SECONDS = 5

# fundamentals_data = {} 

# NY_TZ = pytz.timezone('America/New_York')

# MINIO_URL = os.getenv("S3_ENDPOINT")
# MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
# MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
# MINIO_SECURE = False

# def load_fundamental_data():
#     """Carica i dati fondamentali delle aziende da MinIO."""
#     print(" [INIT] Loading fundamental data from MinIO...", file=sys.stderr)
    
#     min_required_tickers = len(TOP_TICKERS)
    
#     while len(fundamentals_data) < min_required_tickers:
#         try:
#             minio_client = Minio(
#                 MINIO_URL,
#                 access_key=MINIO_ACCESS_KEY,
#                 secret_key=MINIO_SECRET_KEY,
#                 secure=MINIO_SECURE
#             )
        
#             bucket_name = "company-fundamentals"
        
#             if not minio_client.bucket_exists(bucket_name):
#                 print(f"[ERROR] MinIO bucket '{bucket_name}' does not exist. Retrying in 5 seconds...", file=sys.stderr)
#                 time.sleep(5)
#                 continue

#             for ticker in TOP_TICKERS:
#                 if ticker in fundamentals_data:
#                     continue

#                 object_name = f"{ticker}/2024.parquet"
#                 response = None
#                 try:
#                     response = minio_client.get_object(bucket_name, object_name)
                    
#                     parquet_bytes = io.BytesIO(response.read())
#                     parquet_bytes.seek(0)
#                     df = pd.read_parquet(parquet_bytes)
                
#                     if not df.empty:
#                         row = df.iloc[0]
#                         # Ensure all values are converted to standard Python types (float, int)
#                         # Added more robust handling for potential non-numeric values
#                         eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) and pd.api.types.is_numeric_dtype(type(row.get("eps"))) else None
#                         fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) and pd.api.types.is_numeric_dtype(type(row.get("cashflow_freeCashFlow"))) else None
#                         revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) and pd.api.types.is_numeric_dtype(type(row.get("revenue"))) else None
#                         net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) and pd.api.types.is_numeric_dtype(type(row.get("netIncome"))) else None
#                         debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) and pd.api.types.is_numeric_dtype(type(row.get("balance_totalDebt"))) else None
#                         equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) and pd.api.types.is_numeric_dtype(type(row.get("balance_totalStockholdersEquity"))) else None

#                         profit_margin = net_income / revenue if revenue is not None and revenue != 0 else None
#                         debt_to_equity = debt / equity if equity is not None and equity != 0 else None

#                         fundamentals_data[ticker] = {
#                             "eps": eps,
#                             "freeCashFlow": fcf,
#                             "profit_margin": profit_margin,
#                             "debt_to_equity": debt_to_equity
#                         }
#                         print(f"[FUNDAMENTALS] Loaded data for {ticker}: {fundamentals_data[ticker]}", file=sys.stderr)

#                 except S3Error as e:
#                     print(f"[ERROR] MinIO S3 Error for {ticker} ({object_name}): {e}", file=sys.stderr)
#                 except Exception as e:
#                     print(f"[ERROR] Could not load fundamental data for {ticker} from MinIO ({object_name}): {e}", file=sys.stderr)
#                 finally:
#                     if response:
#                         response.close()
#                         response.release_conn()

#             if len(fundamentals_data) < min_required_tickers:
#                 missing_count = min_required_tickers - len(fundamentals_data)
#                 print(f"[WAIT] Still missing {missing_count} fundamental data entries. Retrying in 5 seconds...", file=sys.stderr)
#                 time.sleep(5)

#         except Exception as e:
#             print(f"[CRITICAL] Failed to initialize Minio client or load any fundamental data: {e}", file=sys.stderr)
#             time.sleep(5)
    
#     print(f" [INIT] Fundamental data loading complete. Loaded {len(fundamentals_data)} entries.", file=sys.stderr)


# class SlidingAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         # States for REAL trade data
#         self.real_price_1m = runtime_context.get_map_state(descriptor("real_price_1m"))
#         self.real_price_5m = runtime_context.get_map_state(descriptor("real_price_5m"))
#         self.real_price_30m = runtime_context.get_map_state(descriptor("real_price_30m"))

#         self.real_size_1m = runtime_context.get_map_state(descriptor("real_size_1m"))
#         self.real_size_5m = runtime_context.get_map_state(descriptor("real_size_5m"))
#         self.real_size_30m = runtime_context.get_map_state(descriptor("real_size_30m"))

#         # States for FAKE (simulated) trade data
#         self.fake_price_1m = runtime_context.get_map_state(descriptor("fake_price_1m"))
#         self.fake_price_5m = runtime_context.get_map_state(descriptor("fake_price_5m"))
#         self.fake_price_30m = runtime_context.get_map_state(descriptor("fake_price_30m"))

#         self.fake_size_1m = runtime_context.get_map_state(descriptor("fake_size_1m"))
#         self.fake_size_5m = runtime_context.get_map_state(descriptor("fake_size_5m"))
#         self.fake_size_30m = runtime_context.get_map_state(descriptor("fake_size_30m"))

#         # States for sentiment (specific per ticker)
#         self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
#         self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

#         self.last_timer_state = runtime_context.get_state(
#             ValueStateDescriptor("last_timer", Types.LONG()))

#         # NEW: State to track if timer has been initialized for this key
#         self.timer_initialized_state = runtime_context.get_state(
#             ValueStateDescriptor("timer_initialized", Types.BOOLEAN()))

#         # NEW: States to store last valid price means
#         self.last_real_price_mean_1m_state = runtime_context.get_state(
#             ValueStateDescriptor("last_real_price_mean_1m", Types.FLOAT()))
#         self.last_real_price_mean_5m_state = runtime_context.get_state(
#             ValueStateDescriptor("last_real_price_mean_5m", Types.FLOAT()))
#         self.last_real_price_mean_30m_state = runtime_context.get_state(
#             ValueStateDescriptor("last_real_price_mean_30m", Types.FLOAT()))
        
#         # Stati fallback SEPARATI per simulati
#         self.last_fake_price_mean_1m_state = runtime_context.get_state(
#             ValueStateDescriptor("last_fake_price_mean_1m", Types.FLOAT()))
#         self.last_fake_price_mean_5m_state = runtime_context.get_state(
#             ValueStateDescriptor("last_fake_price_mean_5m", Types.FLOAT()))
#         self.last_fake_price_mean_30m_state = runtime_context.get_state(
#             ValueStateDescriptor("last_fake_price_mean_30m", Types.FLOAT()))
        
#         # NEW: State to track if we've received at least one stock data point
#         self.has_stock_data_state = runtime_context.get_state(
#             ValueStateDescriptor("has_stock_data", Types.BOOLEAN()))

#     def _cleanup_old_entries(self, state, window_minutes):
#         """Rimuove le entry dallo stato più vecchie della finestra specificata."""
#         threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
#         keys_to_remove = []
#         # It's crucial to convert the iterator to a list before iterating
#         # to avoid ConcurrentModificationException if state is modified during iteration,
#         # and to allow multiple passes if needed (though not directly here).
#         for k in list(state.keys()): 
#             try:
#                 dt_obj = isoparse(k)
#                 if dt_obj.tzinfo is None:
#                     dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                
#                 if dt_obj < threshold:
#                     keys_to_remove.append(k)
#             except ValueError:
#                 print(f"[WARN] Invalid timestamp format '{k}' in state for cleanup. Removing.", file=sys.stderr)
#                 keys_to_remove.append(k)
#             except Exception as e:
#                 print(f"[ERROR] Unexpected error during cleanup for key '{k}': {e}. Removing.", file=sys.stderr)
#                 keys_to_remove.append(k)
        
#         for k_remove in keys_to_remove:
#             state.remove(k_remove)

#     def _register_next_timer(self, ctx):
#         """Registra il prossimo timer ogni 10 secondi esatti sincronizzato sui secondi tondi."""
#         current_processing_time = ctx.timer_service().current_processing_time()
        
#         # Convert to datetime to work with seconds
#         current_dt = datetime.fromtimestamp(current_processing_time / 1000, tz=timezone.utc)
        
#         # Calculate next 10-second boundary (0, 10, 20, 30, 40, 50 seconds)
#         current_second = current_dt.second
#         seconds_to_next_boundary = (10 - (current_second % 10)) % 10
        
#         # If we're exactly on a boundary, schedule for the next one
#         if seconds_to_next_boundary == 0:
#             seconds_to_next_boundary = 10
            
#         next_dt = current_dt.replace(microsecond=0) + timedelta(seconds=seconds_to_next_boundary)
#         next_ts = int(next_dt.timestamp() * 1000)  # Convert back to milliseconds
        
#         ctx.timer_service().register_processing_time_timer(next_ts)
#         self.last_timer_state.update(next_ts)
        
#         print(f"[TIMER] Next timer scheduled for {next_dt.strftime('%H:%M:%S')} (in {seconds_to_next_boundary}s)", file=sys.stderr)

#     def _register_first_timer_on_boundary(self, ctx):
#         """Registra il primo timer sul prossimo boundary di 10 secondi dopo aver ricevuto il primo dato."""
#         current_processing_time = ctx.timer_service().current_processing_time()
#         current_dt = datetime.fromtimestamp(current_processing_time / 1000, tz=timezone.utc)
        
#         # Calculate next 10-second boundary (0, 10, 20, 30, 40, 50 seconds)
#         current_second = current_dt.second
#         seconds_to_next_boundary = (10 - (current_second % 10)) % 10
        
#         # If we're exactly on a boundary, schedule for the next one
#         if seconds_to_next_boundary == 0:
#             seconds_to_next_boundary = 10
            
#         next_dt = current_dt.replace(microsecond=0) + timedelta(seconds=seconds_to_next_boundary)
#         next_ts = int(next_dt.timestamp() * 1000)
        
#         ctx.timer_service().register_processing_time_timer(next_ts)
#         self.last_timer_state.update(next_ts)
        
#         print(f"[TIMER] First timer scheduled for {next_dt.strftime('%H:%M:%S')} (in {seconds_to_next_boundary}s)", file=sys.stderr)

#     def process_element(self, value, ctx):
#         """Elabora ogni elemento in ingresso (stringa JSON) da Kafka."""
#         try:
#             data = json.loads(value)
#             current_key = ctx.get_current_key()

#             # --- Gestione dei dati di sentiment (specifici per ticker) ---
#             if "social" in data and "sentiment_score" in data:
#                 social_source = data.get("social")
#                 sentiment_score = float(data.get("sentiment_score"))
#                 ts_str = data.get("timestamp")
                
#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in sentiment data: {data}", file=sys.stderr)
#                     return []

#                 # Assumiamo che qui arrivino solo sentiment per ticker specifici
#                 # o che "GENERAL" sia già stato filtrato/gestito prima di questo operatore keyed
#                 if current_key in TOP_TICKERS:
#                     if social_source == "bluesky":
#                         self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
#                         self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
#                     elif social_source == "news":
#                         self.sentiment_news_1d.put(ts_str, sentiment_score)
#                         self.sentiment_news_3d.put(ts_str, sentiment_score)
#                     else:
#                         print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
#                         return []
#                     # print(f"[SENTIMENT] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr) # Rimosso per ridurre la verbosità
#                 return []

#             # --- Gestione dei dati di Stock Trade ---
#             elif "price" in data and "size" in data and "exchange" in data:
#                 ticker = data.get("ticker")
#                 if ticker not in TOP_TICKERS:
#                     # Questo dovrebbe essere già filtrato dalla key_by, ma è un fallback
#                     return []
                
#                 ts_str = data.get("timestamp")
#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
#                     return []

#                 price = float(data.get("price"))
#                 size = float(data.get("size"))
#                 exchange = data.get("exchange")

#                 # Mark that we've received stock data for this ticker
#                 if not self.has_stock_data_state.value():
#                     self.has_stock_data_state.update(True)
#                     print(f"[STOCK_DATA] First stock data received for ticker {current_key}", file=sys.stderr)

#                 # Initialize timer for this key if not already done AND we have stock data
#                 if not self.timer_initialized_state.value() and self.has_stock_data_state.value():
#                     self._register_first_timer_on_boundary(ctx)
#                     self.timer_initialized_state.update(True)
#                     print(f"[TIMER] Initialized timer for ticker {current_key} on next 10s boundary", file=sys.stderr)

#                 if exchange != "RANDOM": # Dati reali
#                     self.real_price_1m.put(ts_str, price)
#                     self.real_price_5m.put(ts_str, price)
#                     self.real_price_30m.put(ts_str, price)
#                     self.real_size_1m.put(ts_str, size)
#                     self.real_size_5m.put(ts_str, size)
#                     self.real_size_30m.put(ts_str, size)
#                 else: # Dati simulati
#                     self.fake_price_1m.put(ts_str, price)
#                     self.fake_price_5m.put(ts_str, price)
#                     self.fake_price_30m.put(ts_str, price)
#                     self.fake_size_1m.put(ts_str, size)
#                     self.fake_size_5m.put(ts_str, size)
#                     self.fake_size_30m.put(ts_str, size)
#                 return []

#             else:
#                 print(f"[WARN] Unrecognized data format in main job process_element: {value}", file=sys.stderr)
#                 return []

#         except json.JSONDecodeError:
#             print(f"[ERROR] Failed to decode JSON in main job process_element: {value}", file=sys.stderr)
#             return []
#         except Exception as e:
#             print(f"[ERROR] process_element in main job: {e} for value: {value}", file=sys.stderr)
#             return []

#     def on_timer(self, timestamp, ctx):
#         """Chiamata quando un timer registrato scatta."""
#         try:
#             # Converte il timestamp del timer (millisecondi) in un oggetto datetime UTC
#             # e poi lo formatta come stringa ISO per la stampa e l'output
#             ts_prediction = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
#             ts_str = ts_prediction.isoformat()
            
#             ticker = ctx.get_current_key()

#             # Solo i ticker nella lista TOP_TICKERS dovrebbero attivare questo timer
#             if ticker not in TOP_TICKERS:
#                 return [] # Non processare chiavi non pertinenti

#             # Don't process if we haven't received any stock data yet
#             if not self.has_stock_data_state.value():
#                 print(f"[TIMER] Skipping prediction for {ticker} - no stock data received yet", file=sys.stderr)
#                 return []

#             # ALWAYS register the next timer FIRST to ensure continuous execution
#             self._register_next_timer(ctx)

#             # Funzioni helper
#             # Convert RemovableConcatIterator to list explicitly here for robustness
#             def mean(vals_iterator):
#                 vals_list = list(vals_iterator)
#                 return float(np.mean(vals_list)) if vals_list else 0.0

#             def mean_with_last_fallback(vals_iterator, last_value_state):
#                 """Calcola la media, se non ci sono dati usa l'ultimo valore salvato"""
#                 vals_list = list(vals_iterator)
#                 if len(vals_list) == 0:
#                     # Se non ci sono dati, usa l'ultimo valore salvato
#                     last_val = last_value_state.value()
#                     return float(last_val) if last_val is not None else None
#                 elif len(vals_list) == 1:
#                     current_val = float(vals_list[0])
#                     last_value_state.update(current_val)  # Aggiorna l'ultimo valore
#                     return current_val
#                 else:
#                     current_val = float(np.mean(vals_list))
#                     last_value_state.update(current_val)  # Aggiorna l'ultimo valore
#                     return current_val

#             def std(vals_iterator):
#                 vals_list = list(vals_iterator)
#                 return float(np.std(vals_list)) if vals_list and len(vals_list) > 1 else 0.0

#             def total(vals_iterator):
#                 vals_list = list(vals_iterator)
#                 return float(np.sum(vals_list)) if vals_list else 0.0

#             # Pulizia per TUTTI gli stati (reali e simulati)
#             self._cleanup_old_entries(self.real_price_1m, 1)
#             self._cleanup_old_entries(self.real_price_5m, 5)
#             self._cleanup_old_entries(self.real_price_30m, 30)
#             self._cleanup_old_entries(self.real_size_1m, 1)
#             self._cleanup_old_entries(self.real_size_5m, 5)
#             self._cleanup_old_entries(self.real_size_30m, 30)

#             self._cleanup_old_entries(self.fake_price_1m, 1)
#             self._cleanup_old_entries(self.fake_price_5m, 5)
#             self._cleanup_old_entries(self.fake_price_30m, 30)
#             self._cleanup_old_entries(self.fake_size_1m, 1)
#             self._cleanup_old_entries(self.fake_size_5m, 5)
#             self._cleanup_old_entries(self.fake_size_30m, 30)

#             # Pulizia del sentiment specifico (invariata)
#             self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
#             self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)

#             # Usa il timestamp del timer per i calcoli orari, convertito in NY_TZ
#             now_ny = ts_prediction.astimezone(NY_TZ) # Usa ts_prediction per ora NY
#             market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#             market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
            
#             is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5 # Lun-Ven

#             is_simulated_prediction = False
#             if is_market_hours:
#                 # Usa dati reali con fallback ai reali
#                 price_1m_values = list(self.real_price_1m.values())
#                 price_5m_values = list(self.real_price_5m.values())
#                 price_30m_values = list(self.real_price_30m.values())
#                 size_1m_values = list(self.real_size_1m.values())
#                 size_5m_values = list(self.real_size_5m.values())
#                 size_30m_values = list(self.real_size_30m.values())
#                 is_simulated_prediction = False
#             else:
#                 # Usa dati simulati con fallback ai simulati
#                 price_1m_values = list(self.fake_price_1m.values())
#                 price_5m_values = list(self.fake_price_5m.values())
#                 price_30m_values = list(self.fake_price_30m.values())
#                 size_1m_values = list(self.fake_size_1m.values())
#                 size_5m_values = list(self.fake_size_5m.values())
#                 size_30m_values = list(self.fake_size_30m.values())
#                 is_simulated_prediction = True

#             # Calcola i flag di spike all'apertura/chiusura del mercato
#             market_open_spike_flag = 0
#             market_close_spike_flag = 0

#             if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
#                 market_open_spike_flag = 1
            
#             if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
#                 market_close_spike_flag = 1

#             ticker_fundamentals = fundamentals_data.get(ticker, {})

#             # Gestione di "minutes_since_open"
#             minutes_since_open = -1 # Valore di default
#             if now_ny >= market_open_time and now_ny < market_close_time:
#                 minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
#             else:
#                 if now_ny < market_open_time: # Dalla mezzanotte all'apertura
#                     minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
#                     minutes_since_open = -(minutes_until_open) # Valore negativo per "prima dell'apertura"
#                 else: # Dopo la chiusura del mercato
#                     minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + (16*60 - 9*60 - 30) # Minuti dalla chiusura + durata del mercato

#             # Calculate price means with fallback to last value
#             # Calcola le medie con fallback separati
#             if is_market_hours:
#                 # Usa fallback ai dati reali
#                 price_mean_1min = mean_with_last_fallback(price_1m_values, self.last_real_price_mean_1m_state)
#                 price_mean_5min = mean_with_last_fallback(price_5m_values, self.last_real_price_mean_5m_state) 
#                 price_mean_30min = mean_with_last_fallback(price_30m_values, self.last_real_price_mean_30m_state)
#             else:
#                 # Usa fallback ai dati simulati
#                 price_mean_1min = mean_with_last_fallback(price_1m_values, self.last_fake_price_mean_1m_state)
#                 price_mean_5min = mean_with_last_fallback(price_5m_values, self.last_fake_price_mean_5m_state) 
#                 price_mean_30min = mean_with_last_fallback(price_30m_values, self.last_fake_price_mean_30m_state)
                        
#             # Skip prediction if we don't have any price data yet
#             if price_mean_1min is None or price_mean_5min is None or price_mean_30min is None:
#                 print(f"[TIMER] Skipping prediction for {ticker} - no price data available yet", file=sys.stderr)
#                 return []

#             features = {
#                 "ticker": ticker,
#                 "timestamp": ts_str,
#                 "price_mean_1min": price_mean_1min,
#                 "price_mean_5min": price_mean_5min,
#                 "price_std_5min": std(price_5m_values),
#                 "price_mean_30min": price_mean_30min,
#                 "price_std_30min": std(price_30m_values),
#                 "size_tot_1min": total(size_1m_values),
#                 "size_tot_5min": total(size_5m_values),
#                 "size_tot_30min": total(size_30m_values),
#                 # SENTIMENT SPECIFICO PER TICKER
#                 "sentiment_bluesky_mean_2hours": mean(list(self.sentiment_bluesky_2h.values())), # Ensure list conversion
#                 "sentiment_bluesky_mean_1day": mean(list(self.sentiment_bluesky_1d.values())), # Ensure list conversion
#                 "sentiment_news_mean_1day": mean(list(self.sentiment_news_1d.values())),       # Ensure list conversion
#                 "sentiment_news_mean_3days": mean(list(self.sentiment_news_3d.values())),       # Ensure list conversion
#                 # NEW TIME-BASED FEATURES
#                 "minutes_since_open": int(minutes_since_open),
#                 "day_of_week": int(now_ny.weekday()),
#                 "day_of_month": int(now_ny.day),
#                 "week_of_year": int(now_ny.isocalendar()[1]),
#                 "month_of_year": int(now_ny.month),
#                 "market_open_spike_flag": int(market_open_spike_flag),
#                 "market_close_spike_flag": int(market_close_spike_flag),
#                 # Dati fondamentali
#                 "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
#                 "free_cash_flow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
#                 "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
#                 "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None,
#                 # Flag per indicare se la predizione è basata su dati simulati
#                 "is_simulated_prediction": is_simulated_prediction
#             }

#             result = json.dumps(features)
#             print(f"[PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             return [result]
#         except Exception as e:
#             print(f"[ERROR] on_timer for ticker {ticker}: {e}", file=sys.stderr)
#             # ALWAYS register the next timer even in case of error to maintain continuous execution
#             try:
#                 self._register_next_timer(ctx)
#             except:
#                 pass
#             return [json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)})]
        
# # --- Helper per splittare i dati di sentiment ---
# def expand_sentiment_data(json_str):
#     """
#     Espande una singola stringa JSON di sentiment in più, se contiene una lista di ticker.
#     Filtra specificamente il ticker "GENERAL" per assicurarsi che non venga processato qui.
#     """
#     try:
#         data = json.loads(json_str)
        
#         if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
#             expanded_records = []
#             original_ticker_list = data["ticker"]
            
#             for ticker_item in original_ticker_list:
#                 # In questo job, processiamo SOLO i ticker specifici e non "GENERAL"
#                 if ticker_item != "GENERAL" and ticker_item in TOP_TICKERS:
#                     new_record = data.copy()
#                     new_record["ticker"] = ticker_item
#                     expanded_records.append(json.dumps(new_record))
            
#             if not expanded_records:
#                 # Se dopo il filtro non rimangono ticker validi, non emettere nulla
#                 # print(f"[DEBUG] No specific tickers found or all filtered out for: {json_str}", file=sys.stderr)
#                 return []
#             return expanded_records
        
#         # Passa attraverso altri tipi di dati (es. trades) o sentiment già con un singolo ticker
#         if "ticker" in data and data["ticker"] == "GENERAL":
#             # Filtra esplicitamente i messaggi con ticker "GENERAL" qui,
#             # dato che verranno gestiti dal job secondario.
#             return []

#         return [json_str] # Messaggi come trade o sentiment con singolo ticker
#     except json.JSONDecodeError:
#         print(f"[ERROR] Failed to decode JSON in expand_sentiment_data: {json_str}", file=sys.stderr)
#         return []
#     except Exception as e:
#         print(f"[ERROR] expand_sentiment_data: {e} for {json_str}", file=sys.stderr)
#         return []

# def route_by_ticker(json_str):
#     """Determina la chiave per i dati JSON in ingresso."""
#     try:
#         data = json.loads(json_str)
#         # Questo job si occupa solo di dati con un campo 'ticker' specifico
#         if "ticker" in data:
#             if data["ticker"] in TOP_TICKERS:
#                 return data["ticker"]
#             else:
#                 # Messaggi con ticker non tracciati o "GENERAL" (già filtrato da expand_sentiment_data)
#                 # non dovrebbero arrivare qui se le trasformazioni precedenti funzionano
#                 return "discard_key"
#         else:
#             # Messaggi senza 'ticker' (es. macrodata) non dovrebbero arrivare qui
#             # se i flussi sono separati correttamente.
#             print(f"[WARN] Data with no 'ticker' field, discarding in main job: {json_str}", file=sys.stderr)
#             return "discard_key"
#     except json.JSONDecodeError:
#         print(f"[WARN] Failed to decode JSON for key_by in main job: {json_str}", file=sys.stderr)
#         return "invalid_json_key"
#     except Exception as e:
#         print(f"[ERROR] route_by_ticker in main job: {e} for {json_str}", file=sys.stderr)
#         return "error_key"


# def main():
#     load_fundamental_data() # Carica i dati fondamentali una volta all'avvio del job

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1) # Impostiamo la parallelizzazione a 1 per il testing iniziale,
#                             # ma l'obiettivo è aumentarla per lo scaling per ticker.

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_main_job_group',
#         'auto.offset.reset': 'earliest'
#     }

#     # Il consumer per il Job principale legge dai topic con dati specifici per ticker
#     consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "bluesky_sentiment"], # bluesky_sentiment verrà filtrato per i generali
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     producer = FlinkKafkaProducer(
#         topic='main_data', # Topic di output per le predizioni
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
    
#     # Espandiamo e filtriamo i sentiment (escludendo 'GENERAL')
#     expanded_and_filtered_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())

#     # Key by ticker
#     keyed_stream = expanded_and_filtered_stream.key_by(route_by_ticker, key_type=Types.STRING())
    
#     # Processa con SlidingAggregator
#     # Nota: la chiave 'discard_key' verrà gestita dal process function o semplicemente non genererà output significativo
#     processed = keyed_stream.process(SlidingAggregator(), output_type=Types.STRING())
    
#     # Filtra eventuali output indesiderati da "discard_key" se necessario,
#     # anche se il process_element dovrebbe già restituire [] per quei casi.
#     # processed.filter(lambda x: "discard_key" not in x).add_sink(producer)
#     processed.add_sink(producer)

#     env.execute("Main Job: Ticker-Specific Aggregation and Prediction")

# if __name__ == "__main__":
#     main()


































import os
import sys
import json
import numpy as np
from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse
import pytz
import pandas as pd
import io

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from minio import Minio
from minio.error import S3Error
import psycopg2
import time


# Database connection details for fetching tickers - MUST be set as environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def fetch_tickers_from_db():
    max_retries = 50
    delay = 5

    for attempt in range(max_retries):
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
                print("Column 'is_active' not found. Falling back to all distinct tickers.")
                cursor.execute("SELECT DISTINCT ticker FROM companies_info;")

            result = cursor.fetchall()
            tickers = [row[0] for row in result if row[0]]
            cursor.close()
            conn.close()

            if not tickers:
                print("No tickers found in the database.")
            else:
                print(f"Loaded {len(tickers)} tickers from DB.")
            return tickers
        except Exception as e:
            print(f"Database not available, retrying in {delay * (attempt + 1)} seconds... ({e})")
            time.sleep(delay * (attempt + 1))

    print("Failed to connect to database after multiple attempts. Exiting.")
    exit(1)

TOP_TICKERS = fetch_tickers_from_db()
if not TOP_TICKERS:
    print("No tickers available from DB. Exiting.")
    exit(1)


RETRY_DELAY_SECONDS = 5

fundamentals_data = {} 

NY_TZ = pytz.timezone('America/New_York')

MINIO_URL = os.getenv("S3_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_SECURE = False

def load_fundamental_data():
    """Carica i dati fondamentali delle aziende da MinIO."""
    print(" [INIT] Loading fundamental data from MinIO...", file=sys.stderr)
    
    min_required_tickers = len(TOP_TICKERS)
    
    while len(fundamentals_data) < min_required_tickers:
        try:
            minio_client = Minio(
                MINIO_URL,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE
            )
        
            bucket_name = "company-fundamentals"
        
            if not minio_client.bucket_exists(bucket_name):
                print(f"[ERROR] MinIO bucket '{bucket_name}' does not exist. Retrying in 5 seconds...", file=sys.stderr)
                time.sleep(5)
                continue

            for ticker in TOP_TICKERS:
                if ticker in fundamentals_data:
                    continue

                object_name = f"{ticker}/2024.parquet"
                response = None
                try:
                    response = minio_client.get_object(bucket_name, object_name)
                    
                    parquet_bytes = io.BytesIO(response.read())
                    parquet_bytes.seek(0)
                    df = pd.read_parquet(parquet_bytes)
                
                    if not df.empty:
                        row = df.iloc[0]
                        # Ensure all values are converted to standard Python types (float, int)
                        # Added more robust handling for potential non-numeric values
                        eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) and pd.api.types.is_numeric_dtype(type(row.get("eps"))) else None
                        fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) and pd.api.types.is_numeric_dtype(type(row.get("cashflow_freeCashFlow"))) else None
                        revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) and pd.api.types.is_numeric_dtype(type(row.get("revenue"))) else None
                        net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) and pd.api.types.is_numeric_dtype(type(row.get("netIncome"))) else None
                        debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) and pd.api.types.is_numeric_dtype(type(row.get("balance_totalDebt"))) else None
                        equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) and pd.api.types.is_numeric_dtype(type(row.get("balance_totalStockholdersEquity"))) else None

                        profit_margin = net_income / revenue if revenue is not None and revenue != 0 else None
                        debt_to_equity = debt / equity if equity is not None and equity != 0 else None

                        fundamentals_data[ticker] = {
                            "eps": eps,
                            "freeCashFlow": fcf,
                            "profit_margin": profit_margin,
                            "debt_to_equity": debt_to_equity
                        }
                        print(f"[FUNDAMENTALS] Loaded data for {ticker}: {fundamentals_data[ticker]}", file=sys.stderr)

                except S3Error as e:
                    print(f"[ERROR] MinIO S3 Error for {ticker} ({object_name}): {e}", file=sys.stderr)
                except Exception as e:
                    print(f"[ERROR] Could not load fundamental data for {ticker} from MinIO ({object_name}): {e}", file=sys.stderr)
                finally:
                    if response:
                        response.close()
                        response.release_conn()

            if len(fundamentals_data) < min_required_tickers:
                missing_count = min_required_tickers - len(fundamentals_data)
                print(f"[WAIT] Still missing {missing_count} fundamental data entries. Retrying in 5 seconds...", file=sys.stderr)
                time.sleep(5)

        except Exception as e:
            print(f"[CRITICAL] Failed to initialize Minio client or load any fundamental data: {e}", file=sys.stderr)
            time.sleep(5)
    
    print(f" [INIT] Fundamental data loading complete. Loaded {len(fundamentals_data)} entries.", file=sys.stderr)


class SlidingAggregator(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        def descriptor(name):
            return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

        # States for REAL trade data
        self.real_price_1m = runtime_context.get_map_state(descriptor("real_price_1m"))
        self.real_price_5m = runtime_context.get_map_state(descriptor("real_price_5m"))
        self.real_price_30m = runtime_context.get_map_state(descriptor("real_price_30m"))

        self.real_size_1m = runtime_context.get_map_state(descriptor("real_size_1m"))
        self.real_size_5m = runtime_context.get_map_state(descriptor("real_size_5m"))
        self.real_size_30m = runtime_context.get_map_state(descriptor("real_size_30m"))

        # States for FAKE (simulated) trade data
        self.fake_price_1m = runtime_context.get_map_state(descriptor("fake_price_1m"))
        self.fake_price_5m = runtime_context.get_map_state(descriptor("fake_price_5m"))
        self.fake_price_30m = runtime_context.get_map_state(descriptor("fake_price_30m"))

        self.fake_size_1m = runtime_context.get_map_state(descriptor("fake_size_1m"))
        self.fake_size_5m = runtime_context.get_map_state(descriptor("fake_size_5m"))
        self.fake_size_30m = runtime_context.get_map_state(descriptor("fake_size_30m"))

        self.price_1m_minmax = runtime_context.get_map_state(
            MapStateDescriptor("price_1m_minmax", Types.STRING(), Types.FLOAT())
        )

        # States for sentiment (specific per ticker)
        self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
        self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
        self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
        self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

        self.last_timer_state = runtime_context.get_state(
            ValueStateDescriptor("last_timer", Types.LONG()))

        # NEW: State to track if timer has been initialized for this key
        self.timer_initialized_state = runtime_context.get_state(
            ValueStateDescriptor("timer_initialized", Types.BOOLEAN()))

        # NEW: States to store last valid price means
        self.last_real_price_mean_1m_state = runtime_context.get_state(
            ValueStateDescriptor("last_real_price_mean_1m", Types.FLOAT()))
        self.last_real_price_mean_5m_state = runtime_context.get_state(
            ValueStateDescriptor("last_real_price_mean_5m", Types.FLOAT()))
        self.last_real_price_mean_30m_state = runtime_context.get_state(
            ValueStateDescriptor("last_real_price_mean_30m", Types.FLOAT()))
        
        # Stati fallback SEPARATI per simulati
        self.last_fake_price_mean_1m_state = runtime_context.get_state(
            ValueStateDescriptor("last_fake_price_mean_1m", Types.FLOAT()))
        self.last_fake_price_mean_5m_state = runtime_context.get_state(
            ValueStateDescriptor("last_fake_price_mean_5m", Types.FLOAT()))
        self.last_fake_price_mean_30m_state = runtime_context.get_state(
            ValueStateDescriptor("last_fake_price_mean_30m", Types.FLOAT()))
        
        # NEW: State to track if we've received at least one stock data point
        self.has_stock_data_state = runtime_context.get_state(
            ValueStateDescriptor("has_stock_data", Types.BOOLEAN()))

    def _cleanup_old_entries(self, state, window_minutes):
        """Rimuove le entry dallo stato più vecchie della finestra specificata."""
        threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        keys_to_remove = []
        # It's crucial to convert the iterator to a list before iterating
        # to avoid ConcurrentModificationException if state is modified during iteration,
        # and to allow multiple passes if needed (though not directly here).
        for k in list(state.keys()): 
            try:
                dt_obj = isoparse(k)
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                
                if dt_obj < threshold:
                    keys_to_remove.append(k)
            except ValueError:
                print(f"[WARN] Invalid timestamp format '{k}' in state for cleanup. Removing.", file=sys.stderr)
                keys_to_remove.append(k)
            except Exception as e:
                print(f"[ERROR] Unexpected error during cleanup for key '{k}': {e}. Removing.", file=sys.stderr)
                keys_to_remove.append(k)
        
        for k_remove in keys_to_remove:
            state.remove(k_remove)

    def _register_next_timer(self, ctx):
        """Registra il prossimo timer ogni 10 secondi esatti sincronizzato sui secondi tondi."""
        current_processing_time = ctx.timer_service().current_processing_time()
        
        # Convert to datetime to work with seconds
        current_dt = datetime.fromtimestamp(current_processing_time / 1000, tz=timezone.utc)
        
        # Calculate next 10-second boundary (0, 10, 20, 30, 40, 50 seconds)
        current_second = current_dt.second
        seconds_to_next_boundary = (10 - (current_second % 10)) % 10
        
        # If we're exactly on a boundary, schedule for the next one
        if seconds_to_next_boundary == 0:
            seconds_to_next_boundary = 10
            
        next_dt = current_dt.replace(microsecond=0) + timedelta(seconds=seconds_to_next_boundary)
        next_ts = int(next_dt.timestamp() * 1000)  # Convert back to milliseconds
        
        ctx.timer_service().register_processing_time_timer(next_ts)
        self.last_timer_state.update(next_ts)
        
        print(f"[TIMER] Next timer scheduled for {next_dt.strftime('%H:%M:%S')} (in {seconds_to_next_boundary}s)", file=sys.stderr)

    def _register_first_timer_on_boundary(self, ctx):
        """Registra il primo timer sul prossimo boundary di 10 secondi dopo aver ricevuto il primo dato."""
        current_processing_time = ctx.timer_service().current_processing_time()
        current_dt = datetime.fromtimestamp(current_processing_time / 1000, tz=timezone.utc)
        
        # Calculate next 10-second boundary (0, 10, 20, 30, 40, 50 seconds)
        current_second = current_dt.second
        seconds_to_next_boundary = (10 - (current_second % 10)) % 10
        
        # If we're exactly on a boundary, schedule for the next one
        if seconds_to_next_boundary == 0:
            seconds_to_next_boundary = 10
            
        next_dt = current_dt.replace(microsecond=0) + timedelta(seconds=seconds_to_next_boundary)
        next_ts = int(next_dt.timestamp() * 1000)
        
        ctx.timer_service().register_processing_time_timer(next_ts)
        self.last_timer_state.update(next_ts)
        
        print(f"[TIMER] First timer scheduled for {next_dt.strftime('%H:%M:%S')} (in {seconds_to_next_boundary}s)", file=sys.stderr)

    def process_element(self, value, ctx):
        """Elabora ogni elemento in ingresso (stringa JSON) da Kafka."""
        try:
            data = json.loads(value)
            current_key = ctx.get_current_key()

            # --- Gestione dei dati di sentiment (specifici per ticker) ---
            if "social" in data and "sentiment_score" in data:
                social_source = data.get("social")
                sentiment_score = float(data.get("sentiment_score"))
                ts_str = data.get("timestamp")
                
                if not ts_str:
                    print(f"[ERROR] Missing timestamp in sentiment data: {data}", file=sys.stderr)
                    return []

                # Assumiamo che qui arrivino solo sentiment per ticker specifici
                # o che "GENERAL" sia già stato filtrato/gestito prima di questo operatore keyed
                if current_key in TOP_TICKERS:
                    if social_source == "bluesky":
                        self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
                        self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
                    elif social_source == "news":
                        self.sentiment_news_1d.put(ts_str, sentiment_score)
                        self.sentiment_news_3d.put(ts_str, sentiment_score)
                    else:
                        print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
                        return []
                    # print(f"[SENTIMENT] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr) # Rimosso per ridurre la verbosità
                return []

            # --- Gestione dei dati di Stock Trade ---
            elif "price" in data and "size" in data and "exchange" in data:
                ticker = data.get("ticker")
                if ticker not in TOP_TICKERS:
                    # Questo dovrebbe essere già filtrato dalla key_by, ma è un fallback
                    return []
                
                ts_str = data.get("timestamp")
                if not ts_str:
                    print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
                    return []

                price = float(data.get("price"))
                size = float(data.get("size"))
                exchange = data.get("exchange")

                # Mark that we've received stock data for this ticker
                if not self.has_stock_data_state.value():
                    self.has_stock_data_state.update(True)
                    print(f"[STOCK_DATA] First stock data received for ticker {current_key}", file=sys.stderr)

                # Initialize timer for this key if not already done AND we have stock data
                if not self.timer_initialized_state.value() and self.has_stock_data_state.value():
                    self._register_first_timer_on_boundary(ctx)
                    self.timer_initialized_state.update(True)
                    print(f"[TIMER] Initialized timer for ticker {current_key} on next 10s boundary", file=sys.stderr)

                if exchange != "RANDOM": # Dati reali
                    self.real_price_1m.put(ts_str, price)
                    self.real_price_5m.put(ts_str, price)
                    self.real_price_30m.put(ts_str, price)
                    self.real_size_1m.put(ts_str, size)
                    self.real_size_5m.put(ts_str, size)
                    self.real_size_30m.put(ts_str, size)
                    self.price_1m_minmax.put(ts_str, price)
                else: # Dati simulati
                    self.fake_price_1m.put(ts_str, price)
                    self.fake_price_5m.put(ts_str, price)
                    self.fake_price_30m.put(ts_str, price)
                    self.fake_size_1m.put(ts_str, size)
                    self.fake_size_5m.put(ts_str, size)
                    self.fake_size_30m.put(ts_str, size)
                    self.price_1m_minmax.put(ts_str, price)
                return []

            else:
                print(f"[WARN] Unrecognized data format in main job process_element: {value}", file=sys.stderr)
                return []

        except json.JSONDecodeError:
            print(f"[ERROR] Failed to decode JSON in main job process_element: {value}", file=sys.stderr)
            return []
        except Exception as e:
            print(f"[ERROR] process_element in main job: {e} for value: {value}", file=sys.stderr)
            return []

    def on_timer(self, timestamp, ctx):
        """Chiamata quando un timer registrato scatta."""
        try:
            # Converte il timestamp del timer (millisecondi) in un oggetto datetime UTC
            # e poi lo formatta come stringa ISO per la stampa e l'output
            ts_prediction = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            ts_str = ts_prediction.isoformat()
            
            ticker = ctx.get_current_key()

            # Solo i ticker nella lista TOP_TICKERS dovrebbero attivare questo timer
            if ticker not in TOP_TICKERS:
                return [] # Non processare chiavi non pertinenti

            # Don't process if we haven't received any stock data yet
            if not self.has_stock_data_state.value():
                print(f"[TIMER] Skipping prediction for {ticker} - no stock data received yet", file=sys.stderr)
                return []

            # ALWAYS register the next timer FIRST to ensure continuous execution
            self._register_next_timer(ctx)

            # Funzioni helper
            # Convert RemovableConcatIterator to list explicitly here for robustness
            def mean(vals_iterator):
                vals_list = list(vals_iterator)
                return float(np.mean(vals_list)) if vals_list else 0.0

            def mean_with_last_fallback(vals_iterator, last_value_state):
                """Calcola la media, se non ci sono dati usa l'ultimo valore salvato"""
                vals_list = list(vals_iterator)
                if len(vals_list) == 0:
                    # Se non ci sono dati, usa l'ultimo valore salvato
                    last_val = last_value_state.value()
                    return float(last_val) if last_val is not None else None
                elif len(vals_list) == 1:
                    current_val = float(vals_list[0])
                    last_value_state.update(current_val)  # Aggiorna l'ultimo valore
                    return current_val
                else:
                    current_val = float(np.mean(vals_list))
                    last_value_state.update(current_val)  # Aggiorna l'ultimo valore
                    return current_val

            def std(vals_iterator):
                vals_list = list(vals_iterator)
                return float(np.std(vals_list)) if vals_list and len(vals_list) > 1 else 0.0

            def total(vals_iterator):
                vals_list = list(vals_iterator)
                return float(np.sum(vals_list)) if vals_list else 0.0

            # Pulizia per TUTTI gli stati (reali e simulati)
            self._cleanup_old_entries(self.real_price_1m, 1)
            self._cleanup_old_entries(self.real_price_5m, 5)
            self._cleanup_old_entries(self.real_price_30m, 30)
            self._cleanup_old_entries(self.real_size_1m, 1)
            self._cleanup_old_entries(self.real_size_5m, 5)
            self._cleanup_old_entries(self.real_size_30m, 30)

            self._cleanup_old_entries(self.fake_price_1m, 1)
            self._cleanup_old_entries(self.fake_price_5m, 5)
            self._cleanup_old_entries(self.fake_price_30m, 30)
            self._cleanup_old_entries(self.fake_size_1m, 1)
            self._cleanup_old_entries(self.fake_size_5m, 5)
            self._cleanup_old_entries(self.fake_size_30m, 30)

            self._cleanup_old_entries(self.price_1m_minmax, 1)

            # Pulizia del sentiment specifico (invariata)
            self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
            self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
            self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
            self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)

            # Usa il timestamp del timer per i calcoli orari, convertito in NY_TZ
            now_ny = ts_prediction.astimezone(NY_TZ) # Usa ts_prediction per ora NY
            market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
            
            is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5 # Lun-Ven

            anomaly_result = self._check_price_anomaly(ctx, ts_str, ticker)

            is_simulated_prediction = False
            if is_market_hours:
                # Usa dati reali con fallback ai reali
                price_1m_values = list(self.real_price_1m.values())
                price_5m_values = list(self.real_price_5m.values())
                price_30m_values = list(self.real_price_30m.values())
                size_1m_values = list(self.real_size_1m.values())
                size_5m_values = list(self.real_size_5m.values())
                size_30m_values = list(self.real_size_30m.values())
                is_simulated_prediction = False
            else:
                # Usa dati simulati con fallback ai simulati
                price_1m_values = list(self.fake_price_1m.values())
                price_5m_values = list(self.fake_price_5m.values())
                price_30m_values = list(self.fake_price_30m.values())
                size_1m_values = list(self.fake_size_1m.values())
                size_5m_values = list(self.fake_size_5m.values())
                size_30m_values = list(self.fake_size_30m.values())
                is_simulated_prediction = True

            # Calcola i flag di spike all'apertura/chiusura del mercato
            market_open_spike_flag = 0
            market_close_spike_flag = 0

            if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
                market_open_spike_flag = 1
            
            if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
                market_close_spike_flag = 1

            ticker_fundamentals = fundamentals_data.get(ticker, {})

            # Gestione di "minutes_since_open"
            minutes_since_open = -1 # Valore di default
            if now_ny >= market_open_time and now_ny < market_close_time:
                minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
            else:
                if now_ny < market_open_time: # Dalla mezzanotte all'apertura
                    minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
                    minutes_since_open = -(minutes_until_open) # Valore negativo per "prima dell'apertura"
                else: # Dopo la chiusura del mercato
                    minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + (16*60 - 9*60 - 30) # Minuti dalla chiusura + durata del mercato

            # Calculate price means with fallback to last value
            # Calcola le medie con fallback separati
            if is_market_hours:
                # Usa fallback ai dati reali
                price_mean_1min = mean_with_last_fallback(price_1m_values, self.last_real_price_mean_1m_state)
                price_mean_5min = mean_with_last_fallback(price_5m_values, self.last_real_price_mean_5m_state) 
                price_mean_30min = mean_with_last_fallback(price_30m_values, self.last_real_price_mean_30m_state)
            else:
                # Usa fallback ai dati simulati
                price_mean_1min = mean_with_last_fallback(price_1m_values, self.last_fake_price_mean_1m_state)
                price_mean_5min = mean_with_last_fallback(price_5m_values, self.last_fake_price_mean_5m_state) 
                price_mean_30min = mean_with_last_fallback(price_30m_values, self.last_fake_price_mean_30m_state)
                        
            # Skip prediction if we don't have any price data yet
            if price_mean_1min is None or price_mean_5min is None or price_mean_30min is None:
                print(f"[TIMER] Skipping prediction for {ticker} - no price data available yet", file=sys.stderr)
                return []

            features = {
                "ticker": ticker,
                "timestamp": ts_str,
                "price_mean_1min": price_mean_1min,
                "price_mean_5min": price_mean_5min,
                "price_std_5min": std(price_5m_values),
                "price_mean_30min": price_mean_30min,
                "price_std_30min": std(price_30m_values),
                "size_tot_1min": total(size_1m_values),
                "size_tot_5min": total(size_5m_values),
                "size_tot_30min": total(size_30m_values),
                # SENTIMENT SPECIFICO PER TICKER
                "sentiment_bluesky_mean_2hours": mean(list(self.sentiment_bluesky_2h.values())), # Ensure list conversion
                "sentiment_bluesky_mean_1day": mean(list(self.sentiment_bluesky_1d.values())), # Ensure list conversion
                "sentiment_news_mean_1day": mean(list(self.sentiment_news_1d.values())),       # Ensure list conversion
                "sentiment_news_mean_3days": mean(list(self.sentiment_news_3d.values())),       # Ensure list conversion
                # NEW TIME-BASED FEATURES
                "minutes_since_open": int(minutes_since_open),
                "day_of_week": int(now_ny.weekday()),
                "day_of_month": int(now_ny.day),
                "week_of_year": int(now_ny.isocalendar()[1]),
                "month_of_year": int(now_ny.month),
                "market_open_spike_flag": int(market_open_spike_flag),
                "market_close_spike_flag": int(market_close_spike_flag),
                # Dati fondamentali
                "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
                "free_cash_flow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
                "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
                "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None,
                # Flag per indicare se la predizione è basata su dati simulati
                "is_simulated_prediction": is_simulated_prediction
            }

            result = json.dumps(features)
            print(f"[PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

            outputs = [result]
            if anomaly_result:
                outputs.append(anomaly_result)
                print(F"ANOMALIA RILEVATA {ts_str} - {ticker}", file=sys.stderr)
            return outputs
        
        except Exception as e:
            print(f"[ERROR] on_timer for ticker {ticker}: {e}", file=sys.stderr)
            # ALWAYS register the next timer even in case of error to maintain continuous execution
            try:
                self._register_next_timer(ctx)
            except:
                pass
            return [json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)})]
        
    def _check_price_anomaly(self, ctx, ts_str, ticker):
        """Controlla se c'è un'anomalia nei prezzi dell'ultimo minuto"""
        try:
            prices_data = list(self.price_1m_minmax.items())  # [(timestamp, price), ...]
            
            if len(prices_data) < 2:
                return None
                
            # Trova min e max con i loro timestamp
            min_price = float('inf')
            max_price = float('-inf')
            min_timestamp = None
            max_timestamp = None
            
            for ts, price in prices_data:
                if price < min_price:
                    min_price = price
                    min_timestamp = ts
                if price > max_price:
                    max_price = price
                    max_timestamp = ts

            # Controlla se la differenza supera 1
            if max_timestamp < min_timestamp:
                price_diff = min_price - max_price
            else:
                price_diff = max_price - min_price
            if abs(price_diff) > 0.9:
                anomaly_data = {
                    "ticker": ticker,
                    "timestamp": ts_str,
                    "anomaly_type": "price_spike",
                    "min_price": min_price,
                    "max_price": max_price,
                    "min_timestamp": min_timestamp,
                    "max_timestamp": max_timestamp,
                    "price_difference": price_diff
                }
                print(f"[ANOMALY] {ticker} price anomaly detected: diff={price_diff:.2f}", file=sys.stderr)
                return json.dumps(anomaly_data)
                
            return None
            
        except Exception as e:
            print(f"[ERROR] _check_price_anomaly for {ticker}: {e}", file=sys.stderr)
            return None

# --- Helper per splittare i dati di sentiment ---
def expand_sentiment_data(json_str):
    """
    Espande una singola stringa JSON di sentiment in più, se contiene una lista di ticker.
    Filtra specificamente il ticker "GENERAL" per assicurarsi che non venga processato qui.
    """
    try:
        data = json.loads(json_str)
        
        if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
            expanded_records = []
            original_ticker_list = data["ticker"]
            
            for ticker_item in original_ticker_list:
                # In questo job, processiamo SOLO i ticker specifici e non "GENERAL"
                if ticker_item != "GENERAL" and ticker_item in TOP_TICKERS:
                    new_record = data.copy()
                    new_record["ticker"] = ticker_item
                    expanded_records.append(json.dumps(new_record))
            
            if not expanded_records:
                # Se dopo il filtro non rimangono ticker validi, non emettere nulla
                # print(f"[DEBUG] No specific tickers found or all filtered out for: {json_str}", file=sys.stderr)
                return []
            return expanded_records
        
        # Passa attraverso altri tipi di dati (es. trades) o sentiment già con un singolo ticker
        if "ticker" in data and data["ticker"] == "GENERAL":
            # Filtra esplicitamente i messaggi con ticker "GENERAL" qui,
            # dato che verranno gestiti dal job secondario.
            return []

        return [json_str] # Messaggi come trade o sentiment con singolo ticker
    except json.JSONDecodeError:
        print(f"[ERROR] Failed to decode JSON in expand_sentiment_data: {json_str}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[ERROR] expand_sentiment_data: {e} for {json_str}", file=sys.stderr)
        return []

def route_by_ticker(json_str):
    """Determina la chiave per i dati JSON in ingresso."""
    try:
        data = json.loads(json_str)
        # Questo job si occupa solo di dati con un campo 'ticker' specifico
        if "ticker" in data:
            if data["ticker"] in TOP_TICKERS:
                return data["ticker"]
            else:
                # Messaggi con ticker non tracciati o "GENERAL" (già filtrato da expand_sentiment_data)
                # non dovrebbero arrivare qui se le trasformazioni precedenti funzionano
                return "discard_key"
        else:
            # Messaggi senza 'ticker' (es. macrodata) non dovrebbero arrivare qui
            # se i flussi sono separati correttamente.
            print(f"[WARN] Data with no 'ticker' field, discarding in main job: {json_str}", file=sys.stderr)
            return "discard_key"
    except json.JSONDecodeError:
        print(f"[WARN] Failed to decode JSON for key_by in main job: {json_str}", file=sys.stderr)
        return "invalid_json_key"
    except Exception as e:
        print(f"[ERROR] route_by_ticker in main job: {e} for {json_str}", file=sys.stderr)
        return "error_key"


def main():
    load_fundamental_data() # Carica i dati fondamentali una volta all'avvio del job

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Impostiamo la parallelizzazione a 1 per il testing iniziale,
                            # ma l'obiettivo è aumentarla per lo scaling per ticker.

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_stock_main_job_group',
        'auto.offset.reset': 'earliest'
    }

    # Il consumer per il Job principale legge dai topic con dati specifici per ticker
    consumer = FlinkKafkaConsumer(
        topics=["stock_trades", "bluesky_sentiment"], # bluesky_sentiment verrà filtrato per i generali
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    producer = FlinkKafkaProducer(
        topic='main_data', # Topic di output per le predizioni
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    anomaly_producer = FlinkKafkaProducer(
        topic='anomaly_detection',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    
    # Espandiamo e filtriamo i sentiment (escludendo 'GENERAL')
    expanded_and_filtered_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())

    # Key by ticker
    keyed_stream = expanded_and_filtered_stream.key_by(route_by_ticker, key_type=Types.STRING())
    
    # Processa con SlidingAggregator
    # Nota: la chiave 'discard_key' verrà gestita dal process function o semplicemente non genererà output significativo
    processed = keyed_stream.process(SlidingAggregator(), output_type=Types.STRING())
    
    def is_anomaly(json_str):
        try:
            data = json.loads(json_str)
            return "anomaly_type" in data
        except:
            return False

    def is_prediction(json_str):
        try:
            data = json.loads(json_str)
            return "price_mean_1min" in data
        except:
            return False
    
    processed.filter(is_prediction).add_sink(producer)

    processed.filter(is_anomaly).add_sink(anomaly_producer)

    env.execute("Main Job: Ticker-Specific Aggregation and Prediction")

if __name__ == "__main__":
    main()



























