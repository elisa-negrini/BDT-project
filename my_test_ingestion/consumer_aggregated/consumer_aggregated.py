# import os
# import sys
# import json
# from datetime import datetime, timezone, timedelta
# from dateutil.parser import isoparse
# import pytz
# import psycopg2
# from psycopg2 import sql

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.datastream.functions import CoProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

# # --- Configurazione Kafka ---
# KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
# AGGREGATED_DATA_TOPIC = 'aggregated_data'
# STOCK_TRADES_TOPIC = 'stock_trades'
# KAFKA_GROUP_ID = 'flink_postgres_sink_group'

# # --- Configurazione PostgreSQL ---
# POSTGRES_HOST = 'postgre'
# POSTGRES_PORT = 5432
# POSTGRES_DB = 'aggregated-data'
# POSTGRES_USER = 'admin'
# POSTGRES_PASSWORD = 'admin123'
# TABLE_STREAM1 = 'prova_stream1'
# TABLE_STREAM2 = 'prova_stream2'

# # --- Fuso Orario di New York ---
# NY_TZ = pytz.timezone('America/New_York')

# # --- Schemi delle tabelle PostgreSQL ---
# CREATE_TABLE_QUERY_TEMPLATE = sql.SQL("""
#     CREATE TABLE IF NOT EXISTS {table_name} (
#         ticker VARCHAR(10) NOT NULL,
#         timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
#         price_mean_1min DOUBLE PRECISION,
#         price_mean_5min DOUBLE PRECISION,
#         price_std_5min DOUBLE PRECISION,
#         price_mean_30min DOUBLE PRECISION,
#         price_std_30min DOUBLE PRECISION,
#         size_tot_1min DOUBLE PRECISION,
#         size_tot_5min DOUBLE PRECISION,
#         size_tot_30min DOUBLE PRECISION,
#         sentiment_bluesky_mean_2hours DOUBLE PRECISION,
#         sentiment_bluesky_mean_1day DOUBLE PRECISION,
#         sentiment_news_mean_1day DOUBLE PRECISION,
#         sentiment_news_mean_3days DOUBLE PRECISION,
#         sentiment_general_bluesky_mean_2hours DOUBLE PRECISION,
#         sentiment_general_bluesky_mean_1day DOUBLE PRECISION,
#         minutes_since_open DOUBLE PRECISION,
#         day_of_week INTEGER,
#         day_of_month INTEGER,
#         week_of_year INTEGER,
#         month_of_year INTEGER,
#         market_open_spike_flag INTEGER,
#         market_close_spike_flag INTEGER,
#         eps DOUBLE PRECISION,
#         free_cash_flow DOUBLE PRECISION,
#         profit_margin DOUBLE PRECISION,
#         debt_to_equity DOUBLE PRECISION,
#         gdp_real DOUBLE PRECISION,
#         cpi DOUBLE PRECISION,
#         ffr DOUBLE PRECISION,
#         t10y DOUBLE PRECISION,
#         t2y DOUBLE PRECISION,
#         spread_10y_2y DOUBLE PRECISION,
#         unemployment DOUBLE PRECISION,
#         y1 DOUBLE PRECISION,
#         PRIMARY KEY (ticker, timestamp)
#     );
# """)

# # --- Helper per arrotondare il timestamp al minuto più vicino ---
# def round_to_minute(dt_obj):
#     return dt_obj.replace(second=0, microsecond=0)

# # --- Funzione per creare le tabelle in PostgreSQL ---
# def create_tables_if_not_exists():
#     try:
#         conn = psycopg2.connect(
#             host=POSTGRES_HOST,
#             port=POSTGRES_PORT,
#             database=POSTGRES_DB,
#             user=POSTGRES_USER,
#             password=POSTGRES_PASSWORD
#         )
#         cur = conn.cursor()
        
#         print(f"🔗 [DB] Connected to PostgreSQL. Creating tables if not exists...", file=sys.stderr)

#         cur.execute(CREATE_TABLE_QUERY_TEMPLATE.format(table_name=sql.Identifier(TABLE_STREAM1)))
#         print(f"✅ [DB] Table '{TABLE_STREAM1}' ensured.", file=sys.stderr)
        
#         cur.execute(CREATE_TABLE_QUERY_TEMPLATE.format(table_name=sql.Identifier(TABLE_STREAM2)))
#         print(f"✅ [DB] Table '{TABLE_STREAM2}' ensured.", file=sys.stderr)
        
#         conn.commit()
#         cur.close()
#         conn.close()
#     except Exception as e:
#         print(f"❌ [DB ERROR] Could not connect to PostgreSQL or create tables: {e}", file=sys.stderr)
#         sys.exit(1) # Termina lo script se non si connette al DB

# # --- CoProcessFunction per unire e processare i dati ---
# class AggregatedDataProcessor(CoProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         # Stato per i dati aggregati in attesa del loro y1
#         # La chiave è il timestamp arrotondato del dato aggregato
#         self.aggregated_data_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "aggregated_data_state",
#                 Types.STRING(), # timestamp rounded (e.g., "2023-10-27T09:30:00")
#                 Types.STRING()  # full aggregated data JSON string
#             )
#         )
        
#         # Stato per i prezzi di trade che possono essere y1
#         # La chiave è il timestamp arrotondato del trade
#         self.trade_prices_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "trade_prices_state",
#                 Types.STRING(), # timestamp rounded (e.g., "2023-10-27T09:31:00")
#                 Types.LIST(Types.FLOAT()) # lista di prezzi per quel minuto
#             )
#         )

#         # Stato per tracciare se il primo record di un minuto è già stato inviato a prova_stream1
#         # La chiave è il timestamp arrotondato del dato aggregato
#         self.first_record_sent_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "first_record_sent_state",
#                 Types.STRING(), # timestamp rounded
#                 Types.BOOLEAN() # True if first record sent
#             )
#         )
        
#         self.postgres_conn = None
#         self.postgres_cursor = None

#         self._connect_to_postgres()

#     def _connect_to_postgres(self):
#         try:
#             self.postgres_conn = psycopg2.connect(
#                 host=POSTGRES_HOST,
#                 port=POSTGRES_PORT,
#                 database=POSTGRES_DB,
#                 user=POSTGRES_USER,
#                 password=POSTGRES_PASSWORD
#             )
#             self.postgres_cursor = self.postgres_conn.cursor()
#             print(f"🔗 [DB] Successfully reconnected to PostgreSQL.", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [DB ERROR] Failed to connect to PostgreSQL: {e}", file=sys.stderr)
#             # In un ambiente di produzione, qui potresti voler retry o notificare un errore critico

#     def close(self):
#         if self.postgres_cursor:
#             self.postgres_cursor.close()
#         if self.postgres_conn:
#             self.postgres_conn.close()
#         print("🔗 [DB] PostgreSQL connection closed.", file=sys.stderr)

#     def _is_market_hours(self, dt_obj_ny):
#             """Checks if the given NY time object is within market hours (9:30 AM - 3:59 PM NYT) and a weekday."""
            
#             # 1. Check if it's a weekday (Monday=0, Sunday=6)
#             if dt_obj_ny.weekday() >= 7: # 5 is Saturday, 6 is Sunday
#                 return False # Not a weekday, so not market hours

#             # 2. Check if it's within daily market hours
#             market_open = dt_obj_ny.replace(hour=3, minute=30, second=0, microsecond=0)
#             market_close = dt_obj_ny.replace(hour=15, minute=59, second=0, microsecond=0)
            
#             return market_open <= dt_obj_ny < market_close

#     def process_element1(self, value: str, ctx: CoProcessFunction.Context):
#         """Processes elements from the 'aggregated-data' stream."""
#         try:
#             data = json.loads(value)
#             ticker = data.get("ticker")
#             ts_str = data.get("timestamp")
            
#             if not ticker or not ts_str:
#                 print(f"[WARN] Aggregated data missing ticker or timestamp: {value}", file=sys.stderr)
#                 return

#             timestamp_utc = isoparse(ts_str).astimezone(timezone.utc)
#             timestamp_ny = timestamp_utc.astimezone(NY_TZ)

#             if not self._is_market_hours(timestamp_ny):
#                 # print(f"[INFO] Skipping aggregated data outside market hours: {value}", file=sys.stderr)
#                 return

#             # Arrotonda il timestamp al minuto per chiave dello stato e del ground truth
#             # Questo è il timestamp del dato aggregato
#             rounded_ts_ny = round_to_minute(timestamp_ny)
            
#             # Timestamp del ground truth (y1), cioè un minuto dopo
#             y1_ts_ny = rounded_ts_ny + timedelta(minutes=1)

#             # Store the aggregated data, keyed by its *own* rounded timestamp
#             # We use the original timestamp for the state key to avoid collisions within the same minute
#             # and to allow multiple aggregated data points per minute
#             aggregated_data_key = f"{ticker}-{ts_str}" 
#             self.aggregated_data_state.put(aggregated_data_key, value)
            
#             # Register a processing time timer for when the y1 price is expected
#             # (1 minute after the *rounded* timestamp of the aggregated data)
#             timer_timestamp_ms = int(y1_ts_ny.astimezone(timezone.utc).timestamp() * 1000)
#             ctx.timer_service().register_processing_time_timer(timer_timestamp_ms)
            
#             print(f"📝 [AGG] Stored aggregated data for {ticker} @ {ts_str}. Waiting for y1 at {y1_ts_ny.isoformat()}", file=sys.stderr)

#         except json.JSONDecodeError:
#             print(f"❌ [ERROR] Failed to decode JSON from aggregated data: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [ERROR] Processing aggregated data ({value}): {e}", file=sys.stderr)

#     def process_element2(self, value: str, ctx: CoProcessFunction.Context):
#         """Processes elements from the 'stock_trades' stream."""
#         try:
#             data = json.loads(value)
#             ticker = data.get("ticker")
#             ts_str = data.get("timestamp")
#             price = data.get("price")

#             if not ticker or not ts_str or price is None:
#                 print(f"[WARN] Trade data missing ticker, timestamp or price: {value}", file=sys.stderr)
#                 return

#             timestamp_utc = isoparse(ts_str).astimezone(timezone.utc)
#             timestamp_ny = timestamp_utc.astimezone(NY_TZ)

#             if not self._is_market_hours(timestamp_ny):
#                 # print(f"[INFO] Skipping trade data outside market hours: {value}", file=sys.stderr)
#                 return
            
#             # Arrotonda il timestamp del trade al minuto
#             rounded_trade_ts_ny = round_to_minute(timestamp_ny)

#             # Store the trade price, keyed by its rounded timestamp
#             # We want to store all prices for a given minute, to later calculate an average or pick one.
#             # Using a ListState might be better, but MapState with list as value works for simplicity here.
#             current_prices = list(self.trade_prices_state.get(rounded_trade_ts_ny.isoformat()) or [])
#             current_prices.append(float(price))
#             self.trade_prices_state.put(rounded_trade_ts_ny.isoformat(), current_prices)
            
#             print(f"📈 [TRADE] Stored trade price for {ticker} @ {ts_str} (rounded: {rounded_trade_ts_ny.isoformat()})", file=sys.stderr)

#         except json.JSONDecodeError:
#             print(f"❌ [ERROR] Failed to decode JSON from trade data: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [ERROR] Processing trade data ({value}): {e}", file=sys.stderr)

#     def on_timer(self, timestamp: int, ctx: CoProcessFunction.Context):
#         """Called when a registered timer fires."""
#         ticker = ctx.get_current_key()
        
#         # The timer timestamp is in milliseconds UTC
#         timer_timestamp_utc = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
#         timer_timestamp_ny = timer_timestamp_utc.astimezone(NY_TZ)
        
#         # This timer timestamp represents the *y1_ts_ny* we set earlier,
#         # so we are looking for trades that occurred at this minute.
#         y1_expected_minute_ny = timer_timestamp_ny

#         # Check if we have trade prices for this minute (which is the y1 for previous minute's aggregated data)
#         y1_prices = list(self.trade_prices_state.get(y1_expected_minute_ny.isoformat()) or [])

#         # Iterate through all aggregated data points that are waiting for this y1
#         # The key in aggregated_data_state contains the original timestamp (ticker-ts_str)
#         # We need to filter based on whether their *y1_ts_ny* matches the current timer timestamp.
        
#         # Get all aggregated data entries for the current ticker
#         # Note: Iterating and removing from a MapState while iterating can be tricky.
#         # It's safer to build a list of keys to process and then remove.
#         keys_to_process = []
#         for agg_data_key_full in list(self.aggregated_data_state.keys()):
#             # agg_data_key_full format: "TICKER-YYYY-MM-DDTHH:MM:SS.mmmmmm+00:00"
#             parts = agg_data_key_full.split('-', 1) # Split only on the first '-'
#             if len(parts) < 2: continue # Skip malformed keys
            
#             agg_ticker = parts[0]
#             if agg_ticker != ticker: continue # Ensure it's for the current key_by ticker

#             agg_ts_str = parts[1]
#             try:
#                 agg_timestamp_utc = isoparse(agg_ts_str).astimezone(timezone.utc)
#                 agg_timestamp_ny = agg_timestamp_utc.astimezone(NY_TZ)
                
#                 # Calculate the y1 timestamp for this specific aggregated data point
#                 agg_rounded_ts_ny = round_to_minute(agg_timestamp_ny)
#                 agg_y1_ts_ny = agg_rounded_ts_ny + timedelta(minutes=1)

#                 # Check if the y1 timestamp for this aggregated data matches the timer's timestamp
#                 # We compare rounded minutes for this check.
#                 if agg_y1_ts_ny.replace(second=0, microsecond=0) == y1_expected_minute_ny.replace(second=0, microsecond=0):
#                     keys_to_process.append(agg_data_key_full)

#             except ValueError:
#                 print(f"❌ [ERROR] Invalid timestamp format in aggregated data state key: {agg_data_key_full}", file=sys.stderr)
#             except Exception as e:
#                 print(f"❌ [ERROR] Error processing aggregated data key {agg_data_key_full} for timer: {e}", file=sys.stderr)

#         # Process the found aggregated data points
#         if keys_to_process:
#             for agg_data_key_full in keys_to_process:
#                 agg_data_json_str = self.aggregated_data_state.get(agg_data_key_full)
#                 if not agg_data_json_str:
#                     continue # Should not happen, but for safety

#                 agg_data = json.loads(agg_data_json_str)
#                 original_agg_ts_str = agg_data.get("timestamp") # Original timestamp from the aggregated data

#                 # Calculate the y1 value (e.g., mean of prices in that minute)
#                 y1_value = None
#                 if y1_prices:
#                     y1_value = sum(y1_prices) / len(y1_prices)
                
#                 # Add y1 to the aggregated data
#                 agg_data["y1"] = y1_value

#                 # Determine which table to write to (prova_stream1 or prova_stream2)
#                 # Use the rounded timestamp of the *aggregated data* for the first_record_sent_state check
#                 agg_timestamp_utc = isoparse(original_agg_ts_str).astimezone(timezone.utc)
#                 agg_timestamp_ny = agg_timestamp_utc.astimezone(NY_TZ)
#                 agg_rounded_ts_ny = round_to_minute(agg_timestamp_ny)
                
#                 table_to_insert = TABLE_STREAM2 # Default to stream2
#                 first_record_key = f"{ticker}-{agg_rounded_ts_ny.isoformat()}"

#                 if not self.first_record_sent_state.contains(first_record_key) or not self.first_record_sent_state.get(first_record_key):
#                     table_to_insert = TABLE_STREAM1
#                     self.first_record_sent_state.put(first_record_key, True) # Mark as sent

#                 self._write_to_postgres(agg_data, table_to_insert)

#                 # Remove the processed aggregated data from state
#                 self.aggregated_data_state.remove(agg_data_key_full)
#                 print(f"🧹 [STATE] Removed processed aggregated data for {ticker} from state.", file=sys.stderr)

#         # Clear trade prices for this specific minute once they have been used
#         # We can clear trade prices only if ALL aggregated data points for the *previous minute* have been processed.
#         # This is complex because different aggregated data points might have different y1 targets.
#         # For simplicity, we'll clear trade prices for the *timer's minute* after we've used them.
#         # This assumes that all relevant aggregated data for `y1_expected_minute_ny` has already been handled by this timer or earlier.
#         self.trade_prices_state.remove(y1_expected_minute_ny.isoformat())
#         print(f"🧹 [STATE] Cleared trade prices for {ticker} @ {y1_expected_minute_ny.isoformat()}", file=sys.stderr)


#         # Periodically clean up first_record_sent_state to avoid unbounded growth
#         # We can clear entries that are more than, say, 24 hours old.
#         cleanup_threshold_ny = datetime.now(NY_TZ) - timedelta(days=1)
#         keys_to_remove_first_record = []
#         for k in list(self.first_record_sent_state.keys()):
#             try:
#                 # Key format: "TICKER-YYYY-MM-DDTHH:MM:SS"
#                 parts = k.split('-', 1)
#                 if len(parts) < 2: continue
#                 ts_part = parts[1] # The rounded timestamp string
#                 dt_obj_ny = isoparse(ts_part).astimezone(NY_TZ)
#                 if dt_obj_ny < cleanup_threshold_ny:
#                     keys_to_remove_first_record.append(k)
#             except Exception:
#                 keys_to_remove_first_record.append(k) # Remove malformed/unparseable keys

#         for k_remove in keys_to_remove_first_record:
#             self.first_record_sent_state.remove(k_remove)
#             # print(f"🧹 [STATE] Cleaned up first_record_sent_state for {k_remove}", file=sys.stderr)

#     def _write_to_postgres(self, data: dict, table_name: str):
#         if not self.postgres_conn or self.postgres_conn.closed:
#             self._connect_to_postgres() # Attempt to reconnect if connection is lost

#         if not self.postgres_conn or self.postgres_postgres_cursor: # Double check after reconnection attempt
#             print(f"❌ [DB ERROR] No active PostgreSQL connection for writing to {table_name}.", file=sys.stderr)
#             return

#         try:
#             # Assicurati che tutti i campi esistano o siano None
#             values = {
#                 "ticker": data.get("ticker"),
#                 "timestamp": isoparse(data["timestamp"]).astimezone(timezone.utc), # Assicurati che sia in UTC per PostgreSQL
#                 "price_mean_1min": data.get("price_mean_1min"),
#                 "price_mean_5min": data.get("price_mean_5min"),
#                 "price_std_5min": data.get("price_std_5min"),
#                 "price_mean_30min": data.get("price_mean_30min"),
#                 "price_std_30min": data.get("price_std_30min"),
#                 "size_tot_1min": data.get("size_tot_1min"),
#                 "size_tot_5min": data.get("size_tot_5min"),
#                 "size_tot_30min": data.get("size_tot_30min"),
#                 "sentiment_bluesky_mean_2hours": data.get("sentiment_bluesky_mean_2h"), # Nome campo corretto
#                 "sentiment_bluesky_mean_1day": data.get("sentiment_bluesky_mean_1d"),
#                 "sentiment_news_mean_1day": data.get("sentiment_news_mean_1d"),
#                 "sentiment_news_mean_3days": data.get("sentiment_news_mean_3d"),
#                 "sentiment_general_bluesky_mean_2hours": data.get("sentiment_bluesky_mean_general_2hours"),
#                 "sentiment_general_bluesky_mean_1day": data.get("sentiment_bluesky_mean_general_1d"),
#                 "minutes_since_open": data.get("minutes_since_open"),
#                 "day_of_week": data.get("day_of_week"),
#                 "day_of_month": data.get("day_of_month"),
#                 "week_of_year": data.get("week_of_year"),
#                 "month_of_year": data.get("month_of_year"),
#                 "market_open_spike_flag": data.get("market_open_spike_flag"),
#                 "market_close_spike_flag": data.get("market_close_spike_flag"),
#                 "eps": data.get("eps"),
#                 "free_cash_flow": data.get("freeCashFlow"), # Nome campo corretto
#                 "profit_margin": data.get("profit_margin"),
#                 "debt_to_equity": data.get("debt_to_equity"),
#                 "gdp_real": data.get("gdp_real"),
#                 "cpi": data.get("cpi"),
#                 "ffr": data.get("ffr"),
#                 "t10y": data.get("t10y"),
#                 "t2y": data.get("t2y"),
#                 "spread_10y_2y": data.get("spread_10y_2y"),
#                 "unemployment": data.get("unemployment"),
#                 "y1": data.get("y1") # Il valore y1 calcolato
#             }
            
#             # Ordina le chiavi per essere coerenti con la query SQL
#             columns = [
#                 "ticker", "timestamp", "price_mean_1min", "price_mean_5min",
#                 "price_std_5min", "price_mean_30min", "price_std_30min",
#                 "size_tot_1min", "size_tot_5min", "size_tot_30min",
#                 "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
#                 "sentiment_news_mean_1day", "sentiment_news_mean_3days",
#                 "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
#                 "minutes_since_open", "day_of_week", "day_of_month", "week_of_year",
#                 "month_of_year", "market_open_spike_flag", "market_close_spike_flag",
#                 "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
#                 "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
#                 "y1"
#             ]
            
#             # Mappa i valori in base all'ordine delle colonne
#             ordered_values = tuple(values.get(col) for col in columns)

#             insert_query = sql.SQL("""
#                 INSERT INTO {table_name} ({columns})
#                 VALUES ({values})
#                 ON CONFLICT (ticker, timestamp) DO UPDATE SET
#                     price_mean_1min = EXCLUDED.price_mean_1min,
#                     price_mean_5min = EXCLUDED.price_mean_5min,
#                     price_std_5min = EXCLUDED.price_std_5min,
#                     price_mean_30min = EXCLUDED.price_mean_30min,
#                     price_std_30min = EXCLUDED.price_std_30min,
#                     size_tot_1min = EXCLUDED.size_tot_1min,
#                     size_tot_5min = EXCLUDED.size_tot_5min,
#                     size_tot_30min = EXCLUDED.size_tot_30min,
#                     sentiment_bluesky_mean_2hours = EXCLUDED.sentiment_bluesky_mean_2hours,
#                     sentiment_bluesky_mean_1day = EXCLUDED.sentiment_bluesky_mean_1day,
#                     sentiment_news_mean_1day = EXCLUDED.sentiment_news_mean_1day,
#                     sentiment_news_mean_3days = EXCLUDED.sentiment_news_mean_3days,
#                     sentiment_general_bluesky_mean_2hours = EXCLUDED.sentiment_general_bluesky_mean_2hours,
#                     sentiment_general_bluesky_mean_1day = EXCLUDED.sentiment_general_bluesky_mean_1day,
#                     minutes_since_open = EXCLUDED.minutes_since_open,
#                     day_of_week = EXCLUDED.day_of_week,
#                     day_of_month = EXCLUDED.day_of_month,
#                     week_of_year = EXCLUDED.week_of_year,
#                     month_of_year = EXCLUDED.month_of_year,
#                     market_open_spike_flag = EXCLUDED.market_open_spike_flag,
#                     market_close_spike_flag = EXCLUDED.market_close_spike_flag,
#                     eps = EXCLUDED.eps,
#                     free_cash_flow = EXCLUDED.free_cash_flow,
#                     profit_margin = EXCLUDED.profit_margin,
#                     debt_to_equity = EXCLUDED.debt_to_equity,
#                     gdp_real = EXCLUDED.gdp_real,
#                     cpi = EXCLUDED.cpi,
#                     ffr = EXCLUDED.ffr,
#                     t10y = EXCLUDED.t10y,
#                     t2y = EXCLUDED.t2y,
#                     spread_10y_2y = EXCLUDED.spread_10y_2y,
#                     unemployment = EXCLUDED.unemployment,
#                     y1 = EXCLUDED.y1
#             """).format(
#                 table_name=sql.Identifier(table_name),
#                 columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
#                 values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
#             )
            
#             self.postgres_cursor.execute(insert_query, ordered_values)
#             self.postgres_conn.commit()
#             print(f"💾 [DB] Successfully inserted/updated data for {data.get('ticker')} @ {data.get('timestamp')} into {table_name}", file=sys.stderr)

#         except psycopg2.Error as db_error:
#             self.postgres_conn.rollback() # Rollback in case of error
#             print(f"❌ [DB ERROR] Database error writing to {table_name} for {data.get('ticker')}: {db_error}", file=sys.stderr)
#             # Potresti voler riconnetterti in caso di errori specifici di connessione persa
#             if "connection" in str(db_error).lower():
#                 self._connect_to_postgres()
#         except Exception as e:
#             print(f"❌ [DB ERROR] General error writing to {table_name} for {data.get('ticker')}: {e}", file=sys.stderr)


# def main():
#     # Assicurati che le tabelle esistano prima di avviare il job Flink
#     create_tables_if_not_exists()

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1) # Mantieni a 1 per semplicità e per la gestione di global state implicita (anche se per questo job non è un problema)

#     # Consumers per i due topic Kafka
#     aggregated_consumer = FlinkKafkaConsumer(
#         topics=[AGGREGATED_DATA_TOPIC],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#             'group.id': KAFKA_GROUP_ID + "_agg",
#             'auto.offset.reset': 'earliest'
#         }
#     )

#     trades_consumer = FlinkKafkaConsumer(
#         topics=[STOCK_TRADES_TOPIC],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#             'group.id': KAFKA_GROUP_ID + "_trades",
#             'auto.offset.reset': 'earliest'
#         }
#     )

#     agg_stream = env.add_source(aggregated_consumer, type_info=Types.STRING())
#     trades_stream = env.add_source(trades_consumer, type_info=Types.STRING())

#     # Key streams by ticker for co-processing
#     # Important: The key for trades must be just the ticker
#     keyed_agg_stream = agg_stream.key_by(
#         lambda x: json.loads(x).get("ticker"),
#         key_type=Types.STRING()
#     )
#     keyed_trades_stream = trades_stream.key_by(
#         lambda x: json.loads(x).get("ticker"),
#         key_type=Types.STRING()
#     )

#     # CoProcess i due stream
#     # No output type is explicitly needed as we are writing to an external sink
#     keyed_agg_stream.connect(keyed_trades_stream) \
#         .process(AggregatedDataProcessor())

#     env.execute("Flink PostgreSQL Sink Job")

# if __name__ == "__main__":
#     main()











































































## FUNZIONA

# import os
# import sys
# import json
# from datetime import datetime, timezone, timedelta
# from dateutil.parser import isoparse
# import pytz
# import psycopg2
# from psycopg2 import sql, OperationalError, InterfaceError, DatabaseError

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.datastream.functions import CoProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor


# # --- Configurazione Kafka ---
# KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
# AGGREGATED_DATA_TOPIC = 'aggregated_data'
# STOCK_TRADES_TOPIC = 'stock_trades'
# KAFKA_GROUP_ID = 'flink_postgres_sink_group'

# # --- Configurazione PostgreSQL ---
# POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgre')
# POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
# POSTGRES_DB = os.getenv('POSTGRES_DB', 'aggregated-data')
# POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
# POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')
# TABLE_STREAM1 = 'prova_stream1'
# TABLE_STREAM2 = 'prova_stream2'

# # --- Fuso Orario di New York ---
# NY_TZ = pytz.timezone('America/New_York')

# # --- Schemi delle tabelle PostgreSQL ---
# CREATE_TABLE_QUERY_TEMPLATE = sql.SQL("""
#     CREATE TABLE IF NOT EXISTS {table_name} (
#         ticker VARCHAR(10) NOT NULL,
#         timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
#         price_mean_1min DOUBLE PRECISION,
#         price_mean_5min DOUBLE PRECISION,
#         price_std_5min DOUBLE PRECISION,
#         price_mean_30min DOUBLE PRECISION,
#         price_std_30min DOUBLE PRECISION,
#         size_tot_1min DOUBLE PRECISION,
#         size_tot_5min DOUBLE PRECISION,
#         size_tot_30min DOUBLE PRECISION,
#         sentiment_bluesky_mean_2hours DOUBLE PRECISION,
#         sentiment_bluesky_mean_1day DOUBLE PRECISION,
#         sentiment_news_mean_1day DOUBLE PRECISION,
#         sentiment_news_mean_3days DOUBLE PRECISION,
#         sentiment_general_bluesky_mean_2hours DOUBLE PRECISION,
#         sentiment_general_bluesky_mean_1day DOUBLE PRECISION,
#         minutes_since_open DOUBLE PRECISION,
#         day_of_week INTEGER,
#         day_of_month INTEGER,
#         week_of_year INTEGER,
#         month_of_year INTEGER,
#         market_open_spike_flag INTEGER,
#         market_close_spike_flag INTEGER,
#         eps DOUBLE PRECISION,
#         free_cash_flow DOUBLE PRECISION,
#         profit_margin DOUBLE PRECISION,
#         debt_to_equity DOUBLE PRECISION,
#         gdp_real DOUBLE PRECISION,
#         cpi DOUBLE PRECISION,
#         ffr DOUBLE PRECISION,
#         t10y DOUBLE PRECISION,
#         t2y DOUBLE PRECISION,
#         spread_10y_2y DOUBLE PRECISION,
#         unemployment DOUBLE PRECISION,
#         y1 DOUBLE PRECISION,
#         PRIMARY KEY (ticker, timestamp)
#     );
# """)

# # --- Funzione per creare le tabelle in PostgreSQL ---
# def create_tables_if_not_exists():
#     try:
#         conn = psycopg2.connect(
#             host=POSTGRES_HOST,
#             port=POSTGRES_PORT,
#             database=POSTGRES_DB,
#             user=POSTGRES_USER,
#             password=POSTGRES_PASSWORD
#         )
#         cur = conn.cursor()
        
#         print(f"🔗 [DB] Connected to PostgreSQL. Creating tables if not exists...", file=sys.stderr)

#         cur.execute(CREATE_TABLE_QUERY_TEMPLATE.format(table_name=sql.Identifier(TABLE_STREAM1)))
#         print(f"✅ [DB] Table '{TABLE_STREAM1}' ensured.", file=sys.stderr)
        
#         cur.execute(CREATE_TABLE_QUERY_TEMPLATE.format(table_name=sql.Identifier(TABLE_STREAM2)))
#         print(f"✅ [DB] Table '{TABLE_STREAM2}' ensured.", file=sys.stderr)
        
#         conn.commit()
#         cur.close()
#         conn.close()
#     except Exception as e:
#         print(f"❌ [DB ERROR] Could not connect to PostgreSQL or create tables: {e}", file=sys.stderr)
#         sys.exit(1) # Termina lo script se non si connette al DB

# # --- CoProcessFunction per unire e processare i dati ---
# class AggregatedDataProcessor(CoProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         # Stato per i dati aggregati in attesa del loro y1
#         # La chiave è il ticker-timestamp_originale_completo_isoformat del dato aggregato
#         # Il valore è la stringa JSON del dato aggregato
#         self.aggregated_data_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "aggregated_data_state",
#                 Types.STRING(), # ticker-original_timestamp_isoformat (es. "AAPL-2023-10-27T09:30:05-04:00")
#                 Types.STRING()  # Dati aggregati in formato JSON
#             )
#         )
        
#         # Stato per i prezzi di trade
#         # La chiave è il ticker. Il valore è una lista di dict {'timestamp': iso_str, 'price': float}
#         # Questo stato sarà ordinato per timestamp per trovare il primo trade.
#         self.trade_prices_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "trade_prices_state",
#                 Types.STRING(), # Ticker
#                 Types.LIST(Types.MAP(Types.STRING(), Types.STRING())) # [{'timestamp': 'iso_str', 'price': 'str'}]
#             )
#         )

#         # Stato per tracciare se il primo record di un minuto è già stato inviato a prova_stream1
#         # La chiave è ticker-timestamp_arrotondato_del_dato_aggregato
#         self.first_record_sent_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "first_record_sent_state",
#                 Types.STRING(), # ticker-rounded_timestamp_agg_data (e.g., "AAPL-2023-10-27T09:30:00-04:00")
#                 Types.BOOLEAN() # True if first record sent
#             )
#         )
        
#         self.postgres_conn = None
#         self.postgres_cursor = None

#         self._connect_to_postgres()

#     def _connect_to_postgres(self):
#         try:
#             self.postgres_conn = psycopg2.connect(
#                 host=POSTGRES_HOST,
#                 port=POSTGRES_PORT,
#                 database=POSTGRES_DB,
#                 user=POSTGRES_USER,
#                 password=POSTGRES_PASSWORD
#             )
#             self.postgres_cursor = self.postgres_conn.cursor()
#             print(f"🔗 [DB] Successfully reconnected to PostgreSQL.", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [DB ERROR] Failed to connect to PostgreSQL: {e}", file=sys.stderr)

#     def close(self):
#         if self.postgres_cursor:
#             self.postgres_cursor.close()
#         if self.postgres_conn:
#             self.postgres_conn.close()
#         print("🔗 [DB] PostgreSQL connection closed.", file=sys.stderr)

#     def _is_market_hours(self, dt_obj_ny, data_type="aggregated"):
#         """
#         Determina se l'ora corrente è all'interno delle ore di mercato standard (9:30 AM - 4:00 PM ET)
#         e se è un giorno feriale.
#         'data_type' can be "aggregated" or "trade" to apply different end times.
#         """
#         if dt_obj_ny.weekday() >= 5:  # Sabato (5) o Domenica (6)
#             return False
        
#         market_open = dt_obj_ny.replace(hour=9, minute=30, second=0, microsecond=0)
        
#         if data_type == "aggregated":
#             # Per i dati aggregati, salvali solo fino alle 15:58:59.999999 incluso.
#             # Questo si ottiene impostando market_close a 15:59:00.000000 (escluso)
#             market_close = dt_obj_ny.replace(hour=15, minute=59, second=0, microsecond=0) 
#         elif data_type == "trade":
#             # Per i trade, devono essere validi fino alle 16:00:00.999999 incluso,
#             # dato che un trade delle 16:00:00 potrebbe essere il y1 per un aggregato delle 15:59:00 (anche se non lo stori).
#             # Oppure per un aggregato delle 15:58:59 il cui y1 è 15:59:59 o 16:00:00
#             market_close = dt_obj_ny.replace(hour=16, minute=0, second=1, microsecond=0)
#         else: # Default fallback or error
#             market_close = dt_obj_ny.replace(hour=16, minute=0, second=0, microsecond=0) 

#         return market_open <= dt_obj_ny < market_close

#     def _write_to_postgres(self, data: dict, table_name: str):
#         if not self.postgres_conn or self.postgres_conn.closed:
#             print(f"🔗 [DB] Connection to PostgreSQL is closed or None. Attempting to reconnect...", file=sys.stderr)
#             self._connect_to_postgres() # Attempt to reconnect if connection is lost

#         if not self.postgres_conn or not self.postgres_cursor: # Double check after reconnection attempt
#             print(f"❌ [DB ERROR] No active PostgreSQL connection for writing to {table_name}. Skipping data.", file=sys.stderr)
#             return

#         try:
#             # Assicurati che tutti i campi esistano o siano None per il dizionario dei valori
#             values = {k: data.get(k) for k in [
#                 "ticker", "timestamp", "price_mean_1min", "price_mean_5min",
#                 "price_std_5min", "price_mean_30min", "price_std_30min",
#                 "size_tot_1min", "size_tot_5min", "size_tot_30min",
#                 "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
#                 "sentiment_news_mean_1day", "sentiment_news_mean_3days",
#                 "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
#                 "minutes_since_open", "day_of_week", "day_of_month", "week_of_year",
#                 "month_of_year", "market_open_spike_flag", "market_close_spike_flag",
#                 "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
#                 "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
#                 "y1"
#             ]}
            
#             # Correggi i nomi delle chiavi per corrispondere al DB e i valori per i timestamp
#             values["timestamp"] = isoparse(data["timestamp"]).astimezone(timezone.utc)
#             values["sentiment_bluesky_mean_2hours"] = data.get("sentiment_bluesky_mean_2h")
#             values["sentiment_bluesky_mean_1day"] = data.get("sentiment_bluesky_mean_1d")
#             values["sentiment_news_mean_1day"] = data.get("sentiment_news_mean_1d")
#             values["sentiment_news_mean_3days"] = data.get("sentiment_news_mean_3d")
#             values["sentiment_general_bluesky_mean_2hours"] = data.get("sentiment_bluesky_mean_general_2hours")
#             values["sentiment_general_bluesky_mean_1day"] = data.get("sentiment_bluesky_mean_general_1d")
#             values["free_cash_flow"] = data.get("freeCashFlow")


#             # Definisci l'ordine delle colonne per la query INSERT
#             columns = [
#                 "ticker", "timestamp", "price_mean_1min", "price_mean_5min",
#                 "price_std_5min", "price_mean_30min", "price_std_30min",
#                 "size_tot_1min", "size_tot_5min", "size_tot_30min",
#                 "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
#                 "sentiment_news_mean_1day", "sentiment_news_mean_3days",
#                 "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
#                 "minutes_since_open", "day_of_week", "day_of_month", "week_of_year",
#                 "month_of_year", "market_open_spike_flag", "market_close_spike_flag",
#                 "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
#                 "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
#                 "y1"
#             ]
            
#             ordered_values = tuple(values.get(col) for col in columns)

#             insert_query = sql.SQL("""
#                 INSERT INTO {table_name} ({columns})
#                 VALUES ({values})
#                 ON CONFLICT (ticker, timestamp) DO UPDATE SET
#                     price_mean_1min = EXCLUDED.price_mean_1min,
#                     price_mean_5min = EXCLUDED.price_mean_5min,
#                     price_std_5min = EXCLUDED.price_std_5min,
#                     price_mean_30min = EXCLUDED.price_mean_30min,
#                     price_std_30min = EXCLUDED.price_std_30min,
#                     size_tot_1min = EXCLUDED.size_tot_1min,
#                     size_tot_5min = EXCLUDED.size_tot_5min,
#                     size_tot_30min = EXCLUDED.size_tot_30min,
#                     sentiment_bluesky_mean_2hours = EXCLUDED.sentiment_bluesky_mean_2hours,
#                     sentiment_bluesky_mean_1day = EXCLUDED.sentiment_bluesky_mean_1day,
#                     sentiment_news_mean_1day = EXCLUDED.sentiment_news_mean_1day,
#                     sentiment_news_mean_3days = EXCLUDED.sentiment_news_mean_3days,
#                     sentiment_general_bluesky_mean_2hours = EXCLUDED.sentiment_general_bluesky_mean_2hours,
#                     sentiment_general_bluesky_mean_1day = EXCLUDED.sentiment_general_bluesky_mean_1day,
#                     minutes_since_open = EXCLUDED.minutes_since_open,
#                     day_of_week = EXCLUDED.day_of_week,
#                     day_of_month = EXCLUDED.day_of_month,
#                     week_of_year = EXCLUDED.week_of_year,
#                     month_of_year = EXCLUDED.month_of_year,
#                     market_open_spike_flag = EXCLUDED.market_open_spike_flag,
#                     market_close_spike_flag = EXCLUDED.market_close_spike_flag,
#                     eps = EXCLUDED.eps,
#                     free_cash_flow = EXCLUDED.free_cash_flow,
#                     profit_margin = EXCLUDED.profit_margin,
#                     debt_to_equity = EXCLUDED.debt_to_equity,
#                     gdp_real = EXCLUDED.gdp_real,
#                     cpi = EXCLUDED.cpi,
#                     ffr = EXCLUDED.ffr,
#                     t10y = EXCLUDED.t10y,
#                     t2y = EXCLUDED.t2y,
#                     spread_10y_2y = EXCLUDED.spread_10y_2y,
#                     unemployment = EXCLUDED.unemployment,
#                     y1 = EXCLUDED.y1
#             """).format(
#                 table_name=sql.Identifier(table_name),
#                 columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
#                 values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
#             )
            
#             print(f"ℹ️ [DB] Executing query for {data.get('ticker')} into {table_name}...", file=sys.stderr)
#             self.postgres_cursor.execute(insert_query, ordered_values)
#             self.postgres_conn.commit()
#             print(f"💾 [DB] Successfully inserted/updated data for {data.get('ticker')} @ {data.get('timestamp')} into {table_name}", file=sys.stderr)

#         except (OperationalError, InterfaceError) as conn_error:
#             print(f"❌ [DB ERROR] Connection error writing to {table_name} for {data.get('ticker')}: {conn_error}", file=sys.stderr)
#             try:
#                 self.postgres_conn.rollback()
#             except Exception as rb_exc:
#                 print(f"⚠️ [DB] Rollback failed after connection error: {rb_exc}", file=sys.stderr)
#             print(f"🔄 [DB] Attempting to reconnect to PostgreSQL due to connection error.", file=sys.stderr)
#             self._connect_to_postgres()
#         except DatabaseError as db_error:
#             print(f"❌ [DB ERROR] Database specific error writing to {table_name} for {data.get('ticker')}: {db_error}", file=sys.stderr)
#             try:
#                 self.postgres_conn.rollback()
#             except Exception as rb_exc:
#                 print(f"⚠️ [DB] Rollback failed after database error: {rb_exc}", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [DB ERROR] General error writing to {table_name} for {data.get('ticker')}: {e}", file=sys.stderr)
#             if self.postgres_conn:
#                 try:
#                     self.postgres_conn.rollback()
#                 except Exception as rb_exc:
#                     print(f"⚠️ [DB] Rollback failed after general error: {rb_exc}", file=sys.stderr)

#     def process_element1(self, value: str, ctx: CoProcessFunction.Context):
#         """Processes elements from the 'aggregated_data' stream."""
#         try:
#             data = json.loads(value)

#             ticker = data.get("ticker")
#             ts_str = data.get("timestamp") # Original timestamp from aggregated data
            
#             if not ticker or not ts_str:
#                 print(f"[WARN] Aggregated data missing ticker or timestamp: {value}", file=sys.stderr)
#                 return

#             timestamp_utc = isoparse(ts_str).astimezone(timezone.utc)
#             timestamp_ny = timestamp_utc.astimezone(NY_TZ)

#             # ABILITATO IL CONTROLLO ORARI DI MERCATO con parametro "aggregated"
#             if not self._is_market_hours(timestamp_ny, "aggregated"):
#                 print(f"🚫 [AGG] Skipping aggregated data outside market hours for {ticker} @ {ts_str}", file=sys.stderr)
#                 return

#             # Store the aggregated data, keyed by a unique identifier (ticker-original_timestamp)
#             aggregated_data_key = f"{ticker}-{ts_str}" 
#             self.aggregated_data_state.put(aggregated_data_key, value)
            
#             # Register a processing time timer for exactly 1 minute after this aggregated data's timestamp
#             y1_target_timestamp_ny = timestamp_ny + timedelta(minutes=1)
#             timer_timestamp_ms = int(y1_target_timestamp_ny.astimezone(timezone.utc).timestamp() * 1000)
#             ctx.timer_service().register_processing_time_timer(timer_timestamp_ms)
            
#             print(f"📝 [AGG] Stored aggregated data for {ticker} @ {ts_str}. Waiting for y1 at {y1_target_timestamp_ny.isoformat()} (timer: {datetime.fromtimestamp(timer_timestamp_ms/1000, tz=NY_TZ).isoformat()})", file=sys.stderr)

#         except json.JSONDecodeError:
#             print(f"❌ [ERROR] Failed to decode JSON from aggregated data: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [ERROR] Processing aggregated data ({value}): {e}", file=sys.stderr)

#     def process_element2(self, value: str, ctx: CoProcessFunction.Context):
#         """Processes elements from the 'stock_trades' stream."""
#         try:
#             data = json.loads(value)
            
#             # --- FILTRO 'is_simulated_prediction' RIMOSSO ---

#             ticker = data.get("ticker")
#             ts_str = data.get("timestamp")
#             price = data.get("price")

#             if not ticker or not ts_str or price is None:
#                 print(f"[WARN] Trade data missing ticker, timestamp or price: {value}", file=sys.stderr)
#                 return

#             timestamp_utc = isoparse(ts_str).astimezone(timezone.utc)
#             timestamp_ny = timestamp_utc.astimezone(NY_TZ)

#             # ABILITATO IL CONTROLLO ORARI DI MERCATO con parametro "trade"
#             if not self._is_market_hours(timestamp_ny, "trade"):
#                 #print(f"🚫 [TRADE] Skipping trade data outside market hours for {ticker} @ {ts_str}", file=sys.stderr)
#                 return
            
#             # Store the trade price and its exact timestamp, keyed by ticker
#             trade_data_for_state = {'timestamp': ts_str, 'price': str(price)} # Store as string to simplify Flink TypeInfo
            
#             current_trades = list(self.trade_prices_state.get(ticker) or [])
#             current_trades.append(trade_data_for_state)
            
#             # Keep trades sorted by timestamp for easy lookup of the "first" trade
#             current_trades.sort(key=lambda x: isoparse(x['timestamp']))

#             self.trade_prices_state.put(ticker, current_trades)
            
#             print(f"📈 [TRADE] Stored trade price for {ticker} @ {ts_str}", file=sys.stderr)

#         except json.JSONDecodeError:
#             print(f"❌ [ERROR] Failed to decode JSON from trade data: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [ERROR] Processing trade data ({value}): {e}", file=sys.stderr)

#     def on_timer(self, timestamp: int, ctx: CoProcessFunction.Context):
#         """Called when a registered timer fires."""
#         current_ticker = ctx.get_current_key() # This is the ticker the timer was registered for
        
#         # The timer timestamp is in milliseconds UTC
#         y1_trigger_timestamp_utc = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
#         y1_trigger_timestamp_ny = y1_trigger_timestamp_utc.astimezone(NY_TZ)
        
#         print(f"⏰ [TIMER] Timer fired for {current_ticker} @ {y1_trigger_timestamp_ny.isoformat()}", file=sys.stderr)

#         # Iterate through all aggregated data points for the current ticker
#         # and process those whose y1 target is less than or equal to this timer's timestamp.
#         keys_to_process = []
#         for agg_data_key_full in list(self.aggregated_data_state.keys()):
#             parts = agg_data_key_full.split('-', 1) 
#             if len(parts) < 2: 
#                 continue
            
#             agg_ticker_in_key = parts[0]
#             if agg_ticker_in_key != current_ticker: # Process only for the current keyed ticker
#                 continue 

#             agg_ts_str_in_key = parts[1]
#             try:
#                 original_agg_timestamp_utc = isoparse(agg_ts_str_in_key).astimezone(timezone.utc)
#                 original_agg_timestamp_ny = original_agg_timestamp_utc.astimezone(NY_TZ)
                
#                 # Calculate the y1 target timestamp for this specific aggregated data point
#                 agg_y1_target_ts_ny = original_agg_timestamp_ny + timedelta(minutes=1)

#                 # MODIFICA QUI: Se il timestamp del timer è >= del target y1 del dato aggregato, processalo.
#                 if y1_trigger_timestamp_ny >= agg_y1_target_ts_ny:
#                     keys_to_process.append(agg_data_key_full)
#                     print(f"✅ [TIMER MATCH] Dati aggregati {agg_data_key_full} (target y1: {agg_y1_target_ts_ny.isoformat()}) corrispondono al timer a {y1_trigger_timestamp_ny.isoformat()}", file=sys.stderr)
#                 else:
#                     print(f"⏳ [TIMER SKIP] Dati aggregati {agg_data_key_full} (target y1: {agg_y1_target_ts_ny.isoformat()}) sono ancora nel futuro per il timer a {y1_trigger_timestamp_ny.isoformat()}", file=sys.stderr)

#             except ValueError:
#                 print(f"❌ [ERROR] Formato timestamp non valido nella chiave di stato dei dati aggregati: {agg_data_key_full}", file=sys.stderr)
#             except Exception as e:
#                 print(f"❌ [ERROR] Errore nell'elaborazione della chiave di stato dei dati aggregati {agg_data_key_full} per il timer: {e}", file=sys.stderr)

#         # Process the found aggregated data points
#         if keys_to_process:
#             # Get all trades for the current ticker
#             ticker_trades = list(self.trade_prices_state.get(current_ticker) or [])
#             print(f"ℹ️ [TIMER] Per {current_ticker}, lo stato dei trade ha {len(ticker_trades)} trade disponibili per la ricerca di y1.", file=sys.stderr)

#             relevant_trades = []
#             if ticker_trades: 
#                 # Trova il timestamp minimo di destinazione tra le keys_to_process per ottimizzare il filtraggio dei trade
#                 min_agg_y1_target_ts_ny_for_batch = min(
#                     (isoparse(k.split('-', 1)[1]).astimezone(NY_TZ) + timedelta(minutes=1))
#                     for k in keys_to_process
#                 )
#                 for trade_item_str in ticker_trades:
#                     trade_item = trade_item_str
#                     trade_ts_ny = isoparse(trade_item['timestamp']).astimezone(NY_TZ)
#                     # Mantieni solo i trade che sono almeno all'altezza del target y1 minimo del batch
#                     if trade_ts_ny >= min_agg_y1_target_ts_ny_for_batch:
#                         relevant_trades.append({'timestamp_ny': trade_ts_ny, 'price': float(trade_item['price'])})

#             # Ordina i trade rilevanti per assicurarti di trovare il *primo* trade
#             relevant_trades.sort(key=lambda x: x['timestamp_ny'])
#             print(f"ℹ️ [TIMER] Per {current_ticker}, trovati {len(relevant_trades)} trade rilevanti per la ricerca di y1 dopo il filtraggio.", file=sys.stderr)


#             for agg_data_key_full in keys_to_process:
#                 agg_data_json_str = self.aggregated_data_state.get(agg_data_key_full)
#                 if not agg_data_json_str:
#                     print(f"⚠️ [STATE] Dati aggregati per la chiave {agg_data_key_full} non trovati nello stato (potrebbero essere già stati elaborati o errore).", file=sys.stderr)
#                     continue 

#                 agg_data = json.loads(agg_data_json_str)
#                 original_agg_ts_str = agg_data.get("timestamp") 
                
#                 agg_timestamp_utc = isoparse(original_agg_ts_str).astimezone(timezone.utc)
#                 agg_timestamp_ny = agg_timestamp_utc.astimezone(NY_TZ)
#                 current_agg_y1_target_ts_ny = agg_timestamp_ny + timedelta(minutes=1)

#                 y1_value = None
#                 # Trova il primo trade il cui timestamp è >= current_agg_y1_target_ts_ny
#                 for trade in relevant_trades:
#                     # Controlla se il timestamp del trade è al o dopo l'ora target y1 per questo specifico dato aggregato
#                     if trade['timestamp_ny'] >= current_agg_y1_target_ts_ny:
#                         y1_value = trade['price']
#                         print(f"✅ [Y1] Trovato y1 = {y1_value} per {current_ticker} (agg_ts: {original_agg_ts_str}) dal trade a {trade['timestamp_ny'].isoformat()}", file=sys.stderr)
#                         break # Trovato il primo, smetti di cercare

#                 if y1_value is None:
#                     print(f"⚠️ [Y1] Nessun trade trovato per y1 di {current_ticker} (agg_ts: {original_agg_ts_str}) al o dopo {current_agg_y1_target_ts_ny.isoformat()}", file=sys.stderr)

#                 # Aggiungi y1 ai dati aggregati
#                 agg_data["y1"] = y1_value

#                 # Determina in quale tabella scrivere (prova_stream1 o prova_stream2)
#                 agg_rounded_ts_ny = agg_timestamp_ny.replace(second=0, microsecond=0)
                
#                 table_to_insert = TABLE_STREAM2 # Default a stream2
#                 first_record_key = f"{current_ticker}-{agg_rounded_ts_ny.isoformat()}"

#                 if not self.first_record_sent_state.contains(first_record_key) or not self.first_record_sent_state.get(first_record_key):
#                     table_to_insert = TABLE_STREAM1
#                     self.first_record_sent_state.put(first_record_key, True) # Contrassegna come inviato
#                     print(f"⭐ [SAVE] Primo record del minuto per {current_ticker} @ {agg_rounded_ts_ny.isoformat()} andrà a {TABLE_STREAM1}", file=sys.stderr)
#                 else:
#                     print(f"➡️ [SAVE] Record successivo del minuto per {current_ticker} @ {agg_rounded_ts_ny.isoformat()} andrà a {TABLE_STREAM2}", file=sys.stderr)


#                 self._write_to_postgres(agg_data, table_to_insert)

#                 # Rimuovi i dati aggregati elaborati dallo stato
#                 self.aggregated_data_state.remove(agg_data_key_full)
#                 print(f"🧹 [STATE] Rimossi i dati aggregati elaborati {agg_data_key_full} dallo stato.", file=sys.stderr)

#         # Ottimizzazione: Pulisci i trade vecchi dallo stato trade_prices_state
#         cleanup_threshold_trade_ny = y1_trigger_timestamp_ny - timedelta(minutes=5) # Mantieni i trade degli ultimi 5 minuti
        
#         current_trades_for_ticker = list(self.trade_prices_state.get(current_ticker) or [])
#         trades_to_keep = []
#         for trade_item_str in current_trades_for_ticker:
#             trade_ts_ny = isoparse(trade_item_str['timestamp']).astimezone(NY_TZ)
#             if trade_ts_ny >= cleanup_threshold_trade_ny:
#                 trades_to_keep.append(trade_item_str)
        
#         if len(trades_to_keep) < len(current_trades_for_ticker):
#             self.trade_prices_state.put(current_ticker, trades_to_keep)
#             print(f"🧹 [STATE] Puliti i vecchi trade per {current_ticker}. Mantenuti {len(trades_to_keep)} su {len(current_trades_for_ticker)}.", file=sys.stderr)


#         # Pulisci periodicamente first_record_sent_state per evitare una crescita illimitata
#         # Pulisci le voci più vecchie di 24 ore dal tempo di elaborazione corrente (timestamp del timer)
#         cleanup_threshold_first_record_ny = y1_trigger_timestamp_ny - timedelta(days=1) 
#         keys_to_remove_first_record = []
#         for k in list(self.first_record_sent_state.keys()):
#             # Pulisci solo le chiavi per il ticker corrente per evitare contese
#             if not k.startswith(f"{current_ticker}-"):
#                 continue

#             try:
#                 # Formato chiave: "TICKER-YYYY-MM-DDTHH:MM:SS+Offset"
#                 parts = k.split('-', 1)
#                 if len(parts) < 2: continue
#                 ts_part = parts[1] # La stringa del timestamp arrotondato
#                 dt_obj_ny = isoparse(ts_part).astimezone(NY_TZ)
#                 if dt_obj_ny < cleanup_threshold_first_record_ny:
#                     keys_to_remove_first_record.append(k)
#             except Exception as e:
#                 print(f"⚠️ [STATE_CLEAN] Errore nell'analisi della chiave {k} per la pulizia: {e}", file=sys.stderr)
#                 keys_to_remove_first_record.append(k) # Rimuovi chiavi malformate/non parsabili

#         if keys_to_remove_first_record:
#             for k_remove in keys_to_remove_first_record:
#                 self.first_record_sent_state.remove(k_remove)
#                 print(f"🧹 [STATE] Pulita la voce first_record_sent_state per {k_remove}", file=sys.stderr)
            
#         yield # In PyFlink, yield è spesso usato in on_timer anche se non si emette nulla direttamente per conformità

# def main():
#     create_tables_if_not_exists()

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1) 
#     env.enable_checkpointing(5000)

#     aggregated_consumer = FlinkKafkaConsumer(
#         topics=[AGGREGATED_DATA_TOPIC],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#             'group.id': KAFKA_GROUP_ID + "_agg",
#             'auto.offset.reset': 'earliest'
#         }
#     )

#     trades_consumer = FlinkKafkaConsumer(
#         topics=[STOCK_TRADES_TOPIC],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#             'group.id': KAFKA_GROUP_ID + "_trades",
#             'auto.offset.reset': 'earliest'
#         }
#     )

#     agg_stream = env.add_source(aggregated_consumer, type_info=Types.STRING())
#     trades_stream = env.add_source(trades_consumer, type_info=Types.STRING())

#     keyed_agg_stream = agg_stream.key_by(
#         lambda x: json.loads(x).get("ticker"),
#         key_type=Types.STRING()
#     )
#     keyed_trades_stream = trades_stream.key_by(
#         lambda x: json.loads(x).get("ticker"),
#         key_type=Types.STRING()
#     )

#     keyed_agg_stream.connect(keyed_trades_stream) \
#         .process(AggregatedDataProcessor())

#     print("Engine running", file=sys.stderr)
#     env.execute("Flink PostgreSQL Sink Job")

# if __name__ == "__main__":
#     main()



import os
import sys
import json
from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse
import pytz
import psycopg2
from psycopg2 import sql, OperationalError, InterfaceError, DatabaseError

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import CoProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
AGGREGATED_DATA_TOPIC = 'aggregated_data'
STOCK_TRADES_TOPIC = 'stock_trades'
KAFKA_GROUP_ID = 'flink_postgres_sink_group'

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT'))
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
TABLE_STREAM1 = 'aggregated_data'
TABLE_STREAM2 = 'aggregated_data2'

NY_TZ = pytz.timezone('America/New_York')

CREATE_TABLE_QUERY_TEMPLATE = sql.SQL("""
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
""")

def create_tables_if_not_exists():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        
        cur.execute(CREATE_TABLE_QUERY_TEMPLATE.format(table_name=sql.Identifier(TABLE_STREAM1)))
        cur.execute(CREATE_TABLE_QUERY_TEMPLATE.format(table_name=sql.Identifier(TABLE_STREAM2)))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        sys.exit(1)

class AggregatedDataProcessor(CoProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # State for aggregated data awaiting its y1 value.
        # Key: ticker-original_timestamp_isoformat of the aggregated data.
        # Value: JSON string of the aggregated data.
        self.aggregated_data_state = runtime_context.get_map_state(
            MapStateDescriptor(
                "aggregated_data_state",
                Types.STRING(), # ticker-original_timestamp_isoformat (e.g., "AAPL-2023-10-27T09:30:05-04:00")
                Types.STRING()  # Aggregated data in JSON format
            )
        )
        
        # State for trade prices.
        # Key: ticker. Value: list of dicts {'timestamp': iso_str, 'price': float}.
        # This state will be sorted by timestamp to find the first trade.
        self.trade_prices_state = runtime_context.get_map_state(
            MapStateDescriptor(
                "trade_prices_state",
                Types.STRING(), # Ticker
                Types.LIST(Types.MAP(Types.STRING(), Types.STRING())) # [{'timestamp': 'iso_str', 'price': 'str'}]
            )
        )

        # State to track if the first record of a minute has already been sent to prova_stream1.
        # Key: ticker-rounded_timestamp_of_aggregated_data.
        self.first_record_sent_state = runtime_context.get_map_state(
            MapStateDescriptor(
                "first_record_sent_state",
                Types.STRING(), # ticker-rounded_timestamp_agg_data (e.g., "AAPL-2023-10-27T09:30:00-04:00")
                Types.BOOLEAN() # True if first record sent
            )
        )
        
        self.postgres_conn = None
        self.postgres_cursor = None

        self._connect_to_postgres()

    def _connect_to_postgres(self):
        try:
            self.postgres_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            self.postgres_cursor = self.postgres_conn.cursor()
        except Exception as e:
            pass # Error handled silently, will be caught by _write_to_postgres

    def close(self):
        if self.postgres_cursor:
            self.postgres_cursor.close()
        if self.postgres_conn:
            self.postgres_conn.close()

    def _is_market_hours(self, dt_obj_ny, data_type="aggregated"):
        """
        Determines if the current time is within standard market hours (9:30 AM - 4:00 PM ET)
        and if it's a weekday.
        'data_type' can be "aggregated" or "trade" to apply different end times.
        """
        if dt_obj_ny.weekday() >= 5:  # Saturday (5) or Sunday (6)
            return False
        
        market_open = dt_obj_ny.replace(hour=9, minute=30, second=0, microsecond=0)
        
        if data_type == "aggregated":
            # For aggregated data, save only up to 15:58:59.999999 inclusive.
            market_close = dt_obj_ny.replace(hour=15, minute=59, second=0, microsecond=0) 
        elif data_type == "trade":
            # For trades, they must be valid up to 16:00:00.999999 inclusive.
            market_close = dt_obj_ny.replace(hour=16, minute=0, second=1, microsecond=0)
        else:
            market_close = dt_obj_ny.replace(hour=16, minute=0, second=0, microsecond=0) 

        return market_open <= dt_obj_ny < market_close

    def _write_to_postgres(self, data: dict, table_name: str):
        if not self.postgres_conn or self.postgres_conn.closed:
            self._connect_to_postgres()

        if not self.postgres_conn or not self.postgres_cursor:
            return

        try:
            # Ensure all fields exist or are None for the values dictionary
            values = {k: data.get(k) for k in [
                "ticker", "timestamp", "price_mean_1min", "price_mean_5min",
                "price_std_5min", "price_mean_30min", "price_std_30min",
                "size_tot_1min", "size_tot_5min", "size_tot_30min",
                "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
                "sentiment_news_mean_1day", "sentiment_news_mean_3days",
                "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
                "minutes_since_open", "day_of_week", "day_of_month", "week_of_year",
                "month_of_year", "market_open_spike_flag", "market_close_spike_flag",
                "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
                "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
                "y1"
            ]}
            
            # Correct key names for DB and values for timestamps
            values["timestamp"] = isoparse(data["timestamp"]).astimezone(timezone.utc)
            values["sentiment_bluesky_mean_2hours"] = data.get("sentiment_bluesky_mean_2h")
            values["sentiment_bluesky_mean_1day"] = data.get("sentiment_bluesky_mean_1d")
            values["sentiment_news_mean_1day"] = data.get("sentiment_news_mean_1d")
            values["sentiment_news_mean_3days"] = data.get("sentiment_news_mean_3d")
            values["sentiment_general_bluesky_mean_2hours"] = data.get("sentiment_bluesky_mean_general_2hours")
            values["sentiment_general_bluesky_mean_1day"] = data.get("sentiment_bluesky_mean_general_1d")
            values["free_cash_flow"] = data.get("freeCashFlow")

            # Define column order for INSERT query
            columns = [
                "ticker", "timestamp", "price_mean_1min", "price_mean_5min",
                "price_std_5min", "price_mean_30min", "price_std_30min",
                "size_tot_1min", "size_tot_5min", "size_tot_30min",
                "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
                "sentiment_news_mean_1day", "sentiment_news_mean_3days",
                "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
                "minutes_since_open", "day_of_week", "day_of_month", "week_of_year",
                "month_of_year", "market_open_spike_flag", "market_close_spike_flag",
                "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
                "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
                "y1"
            ]
            
            ordered_values = tuple(values.get(col) for col in columns)

            insert_query = sql.SQL("""
                INSERT INTO {table_name} ({columns})
                VALUES ({values})
                ON CONFLICT (ticker, timestamp) DO UPDATE SET
                    price_mean_1min = EXCLUDED.price_mean_1min,
                    price_mean_5min = EXCLUDED.price_mean_5min,
                    price_std_5min = EXCLUDED.price_std_5min,
                    price_mean_30min = EXCLUDED.price_mean_30min,
                    price_std_30min = EXCLUDED.price_std_30min,
                    size_tot_1min = EXCLUDED.size_tot_1min,
                    size_tot_5min = EXCLUDED.size_tot_5min,
                    size_tot_30min = EXCLUDED.size_tot_30min,
                    sentiment_bluesky_mean_2hours = EXCLUDED.sentiment_bluesky_mean_2hours,
                    sentiment_bluesky_mean_1day = EXCLUDED.sentiment_bluesky_mean_1day,
                    sentiment_news_mean_1day = EXCLUDED.sentiment_news_mean_1day,
                    sentiment_news_mean_3days = EXCLUDED.sentiment_news_mean_3days,
                    sentiment_general_bluesky_mean_2hours = EXCLUDED.sentiment_general_bluesky_mean_2hours,
                    sentiment_general_bluesky_mean_1day = EXCLUDED.sentiment_general_bluesky_mean_1day,
                    minutes_since_open = EXCLUDED.minutes_since_open,
                    day_of_week = EXCLUDED.day_of_week,
                    day_of_month = EXCLUDED.day_of_month,
                    week_of_year = EXCLUDED.week_of_year,
                    month_of_year = EXCLUDED.month_of_year,
                    market_open_spike_flag = EXCLUDED.market_open_spike_flag,
                    market_close_spike_flag = EXCLUDED.market_close_spike_flag,
                    eps = EXCLUDED.eps,
                    free_cash_flow = EXCLUDED.free_cash_flow,
                    profit_margin = EXCLUDED.profit_margin,
                    debt_to_equity = EXCLUDED.debt_to_equity,
                    gdp_real = EXCLUDED.gdp_real,
                    cpi = EXCLUDED.cpi,
                    ffr = EXCLUDED.ffr,
                    t10y = EXCLUDED.t10y,
                    t2y = EXCLUDED.t2y,
                    spread_10y_2y = EXCLUDED.spread_10y_2y,
                    unemployment = EXCLUDED.unemployment,
                    y1 = EXCLUDED.y1
            """).format(
                table_name=sql.Identifier(table_name),
                columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            
            self.postgres_cursor.execute(insert_query, ordered_values)
            self.postgres_conn.commit()

        except (OperationalError, InterfaceError) as conn_error:
            try:
                self.postgres_conn.rollback()
            except Exception:
                pass
            self._connect_to_postgres()
        except DatabaseError as db_error:
            try:
                self.postgres_conn.rollback()
            except Exception:
                pass
        except Exception as e:
            if self.postgres_conn:
                try:
                    self.postgres_conn.rollback()
                except Exception:
                    pass

    def process_element1(self, value: str, ctx: CoProcessFunction.Context):
        """Processes elements from the 'aggregated_data' stream."""
        try:
            data = json.loads(value)

            ticker = data.get("ticker")
            ts_str = data.get("timestamp")
            
            if not ticker or not ts_str:
                return

            timestamp_utc = isoparse(ts_str).astimezone(timezone.utc)
            timestamp_ny = timestamp_utc.astimezone(NY_TZ)

            if not self._is_market_hours(timestamp_ny, "aggregated"):
                return

            # Store the aggregated data, keyed by a unique identifier (ticker-original_timestamp)
            aggregated_data_key = f"{ticker}-{ts_str}" 
            self.aggregated_data_state.put(aggregated_data_key, value)
            
            # Register a processing time timer for exactly 1 minute after this aggregated data's timestamp
            y1_target_timestamp_ny = timestamp_ny + timedelta(minutes=1)
            timer_timestamp_ms = int(y1_target_timestamp_ny.astimezone(timezone.utc).timestamp() * 1000)
            ctx.timer_service().register_processing_time_timer(timer_timestamp_ms)
            
        except json.JSONDecodeError:
            pass
        except Exception:
            pass

    def process_element2(self, value: str, ctx: CoProcessFunction.Context):
        """Processes elements from the 'stock_trades' stream."""
        try:
            data = json.loads(value)
            
            ticker = data.get("ticker")
            ts_str = data.get("timestamp")
            price = data.get("price")

            if not ticker or not ts_str or price is None:
                return

            timestamp_utc = isoparse(ts_str).astimezone(timezone.utc)
            timestamp_ny = timestamp_utc.astimezone(NY_TZ)

            if not self._is_market_hours(timestamp_ny, "trade"):
                return
            
            # Store the trade price and its exact timestamp, keyed by ticker
            trade_data_for_state = {'timestamp': ts_str, 'price': str(price)} # Store as string to simplify Flink TypeInfo
            
            current_trades = list(self.trade_prices_state.get(ticker) or [])
            current_trades.append(trade_data_for_state)
            
            # Keep trades sorted by timestamp for easy lookup of the "first" trade
            current_trades.sort(key=lambda x: isoparse(x['timestamp']))

            self.trade_prices_state.put(ticker, current_trades)
            
        except json.JSONDecodeError:
            pass
        except Exception:
            pass

    def on_timer(self, timestamp: int, ctx: CoProcessFunction.Context):
        """Called when a registered timer fires."""
        current_ticker = ctx.get_current_key()
        
        # The timer timestamp is in milliseconds UTC
        y1_trigger_timestamp_utc = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        y1_trigger_timestamp_ny = y1_trigger_timestamp_utc.astimezone(NY_TZ)
        
        keys_to_process = []
        for agg_data_key_full in list(self.aggregated_data_state.keys()):
            parts = agg_data_key_full.split('-', 1) 
            if len(parts) < 2: 
                continue
            
            agg_ticker_in_key = parts[0]
            if agg_ticker_in_key != current_ticker: # Process only for the current keyed ticker
                continue 

            agg_ts_str_in_key = parts[1]
            try:
                original_agg_timestamp_utc = isoparse(agg_ts_str_in_key).astimezone(timezone.utc)
                original_agg_timestamp_ny = original_agg_timestamp_utc.astimezone(NY_TZ)
                
                # Calculate the y1 target timestamp for this specific aggregated data point
                agg_y1_target_ts_ny = original_agg_timestamp_ny + timedelta(minutes=1)

                # If the timer's timestamp is >= the aggregated data's y1 target, process it.
                if y1_trigger_timestamp_ny >= agg_y1_target_ts_ny:
                    keys_to_process.append(agg_data_key_full)
            except ValueError:
                pass
            except Exception:
                pass

        if keys_to_process:
            # Get all trades for the current ticker
            ticker_trades = list(self.trade_prices_state.get(current_ticker) or [])

            relevant_trades = []
            if ticker_trades: 
                # Find the minimum target y1 timestamp among the keys_to_process to optimize trade filtering
                min_agg_y1_target_ts_ny_for_batch = min(
                    (isoparse(k.split('-', 1)[1]).astimezone(NY_TZ) + timedelta(minutes=1))
                    for k in keys_to_process
                )
                for trade_item_str in ticker_trades:
                    trade_item = trade_item_str
                    trade_ts_ny = isoparse(trade_item['timestamp']).astimezone(NY_TZ)
                    # Keep only trades that are at least at the minimum y1 target of the batch
                    if trade_ts_ny >= min_agg_y1_target_ts_ny_for_batch:
                        relevant_trades.append({'timestamp_ny': trade_ts_ny, 'price': float(trade_item['price'])})

            # Sort relevant trades to ensure finding the *first* trade
            relevant_trades.sort(key=lambda x: x['timestamp_ny'])

            for agg_data_key_full in keys_to_process:
                agg_data_json_str = self.aggregated_data_state.get(agg_data_key_full)
                if not agg_data_json_str:
                    continue 

                agg_data = json.loads(agg_data_json_str)
                original_agg_ts_str = agg_data.get("timestamp") 
                
                agg_timestamp_utc = isoparse(original_agg_ts_str).astimezone(timezone.utc)
                agg_timestamp_ny = agg_timestamp_utc.astimezone(NY_TZ)
                current_agg_y1_target_ts_ny = agg_timestamp_ny + timedelta(minutes=1)

                y1_value = None
                # Find the first trade whose timestamp is >= current_agg_y1_target_ts_ny
                for trade in relevant_trades:
                    # Check if the trade timestamp is at or after the y1 target time for this specific aggregated data
                    if trade['timestamp_ny'] >= current_agg_y1_target_ts_ny:
                        y1_value = trade['price']
                        break # Found the first, stop searching

                # Add y1 to the aggregated data
                agg_data["y1"] = y1_value

                # Determine which table to write to (prova_stream1 or prova_stream2)
                agg_rounded_ts_ny = agg_timestamp_ny.replace(second=0, microsecond=0)
                
                table_to_insert = TABLE_STREAM2 # Default to stream2
                first_record_key = f"{current_ticker}-{agg_rounded_ts_ny.isoformat()}"

                if not self.first_record_sent_state.contains(first_record_key) or not self.first_record_sent_state.get(first_record_key):
                    table_to_insert = TABLE_STREAM1
                    self.first_record_sent_state.put(first_record_key, True) # Mark as sent
                
                self._write_to_postgres(agg_data, table_to_insert)

                # Remove processed aggregated data from state
                self.aggregated_data_state.remove(agg_data_key_full)

        # Optimize: Clean up old trades from trade_prices_state
        cleanup_threshold_trade_ny = y1_trigger_timestamp_ny - timedelta(minutes=5) # Keep trades from the last 5 minutes
        
        current_trades_for_ticker = list(self.trade_prices_state.get(current_ticker) or [])
        trades_to_keep = []
        for trade_item_str in current_trades_for_ticker:
            trade_ts_ny = isoparse(trade_item_str['timestamp']).astimezone(NY_TZ)
            if trade_ts_ny >= cleanup_threshold_trade_ny:
                trades_to_keep.append(trade_item_str)
        
        if len(trades_to_keep) < len(current_trades_for_ticker):
            self.trade_prices_state.put(current_ticker, trades_to_keep)
            
        # Periodically clean up first_record_sent_state to prevent unbounded growth
        cleanup_threshold_first_record_ny = y1_trigger_timestamp_ny - timedelta(days=1) 
        keys_to_remove_first_record = []
        for k in list(self.first_record_sent_state.keys()):
            # Only clean up keys for the current ticker to avoid contention
            if not k.startswith(f"{current_ticker}-"):
                continue

            try:
                # Key format: "TICKER-YYYY-MM-DDTHH:MM:SS+Offset"
                parts = k.split('-', 1)
                if len(parts) < 2: continue
                ts_part = parts[1] # The rounded timestamp string
                dt_obj_ny = isoparse(ts_part).astimezone(NY_TZ)
                if dt_obj_ny < cleanup_threshold_first_record_ny:
                    keys_to_remove_first_record.append(k)
            except Exception:
                keys_to_remove_first_record.append(k) # Remove malformed/unparseable keys

        if keys_to_remove_first_record:
            for k_remove in keys_to_remove_first_record:
                self.first_record_sent_state.remove(k_remove)
            
        yield

def main():
    create_tables_if_not_exists()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 
    env.enable_checkpointing(5000)

    aggregated_consumer = FlinkKafkaConsumer(
        topics=[AGGREGATED_DATA_TOPIC],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID + "_agg",
            'auto.offset.reset': 'earliest'
        }
    )

    trades_consumer = FlinkKafkaConsumer(
        topics=[STOCK_TRADES_TOPIC],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID + "_trades",
            'auto.offset.reset': 'earliest'
        }
    )

    agg_stream = env.add_source(aggregated_consumer, type_info=Types.STRING())
    trades_stream = env.add_source(trades_consumer, type_info=Types.STRING())

    keyed_agg_stream = agg_stream.key_by(
        lambda x: json.loads(x).get("ticker"),
        key_type=Types.STRING()
    )
    keyed_trades_stream = trades_stream.key_by(
        lambda x: json.loads(x).get("ticker"),
        key_type=Types.STRING()
    )

    keyed_agg_stream.connect(keyed_trades_stream) \
        .process(AggregatedDataProcessor())

    env.execute("Flink PostgreSQL Sink Job")

if __name__ == "__main__":
    main()











































































# import os
# import sys
# import json
# from datetime import datetime, timezone, timedelta
# from dateutil.parser import isoparse
# import pytz
# import psycopg2
# from psycopg2 import sql

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.datastream.functions import CoProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

# # --- Configurazione Kafka ---
# KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
# AGGREGATED_DATA_TOPIC = 'aggregated_data'
# STOCK_TRADES_TOPIC = 'stock_trades'
# KAFKA_GROUP_ID = 'flink_postgres_sink_group'

# # --- Configurazione PostgreSQL ---
# POSTGRES_HOST = 'postgre'
# POSTGRES_PORT = 5432
# POSTGRES_DB = 'aggregated-data'
# POSTGRES_USER = 'admin'
# POSTGRES_PASSWORD = 'admin123'
# TABLE_STREAM1 = 'prova_stream1'
# TABLE_STREAM2 = 'prova_stream2'

# # --- Fuso Orario di New York ---
# NY_TZ = pytz.timezone('America/New_York')

# # --- Schemi delle tabelle PostgreSQL ---
# CREATE_TABLE_QUERY_TEMPLATE = sql.SQL("""
#     CREATE TABLE IF NOT EXISTS {table_name} (
#         ticker VARCHAR(10) NOT NULL,
#         timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
#         price_mean_1min DOUBLE PRECISION,
#         price_mean_5min DOUBLE PRECISION,
#         price_std_5min DOUBLE PRECISION,
#         price_mean_30min DOUBLE PRECISION,
#         price_std_30min DOUBLE PRECISION,
#         size_tot_1min DOUBLE PRECISION,
#         size_tot_5min DOUBLE PRECISION,
#         size_tot_30min DOUBLE PRECISION,
#         sentiment_bluesky_mean_2hours DOUBLE PRECISION,
#         sentiment_bluesky_mean_1day DOUBLE PRECISION,
#         sentiment_news_mean_1day DOUBLE PRECISION,
#         sentiment_news_mean_3days DOUBLE PRECISION,
#         sentiment_general_bluesky_mean_2hours DOUBLE PRECISION,
#         sentiment_general_bluesky_mean_1day DOUBLE PRECISION,
#         minutes_since_open DOUBLE PRECISION,
#         day_of_week INTEGER,
#         day_of_month INTEGER,
#         week_of_year INTEGER,
#         month_of_year INTEGER,
#         market_open_spike_flag INTEGER,
#         market_close_spike_flag INTEGER,
#         eps DOUBLE PRECISION,
#         free_cash_flow DOUBLE PRECISION,
#         profit_margin DOUBLE PRECISION,
#         debt_to_equity DOUBLE PRECISION,
#         gdp_real DOUBLE PRECISION,
#         cpi DOUBLE PRECISION,
#         ffr DOUBLE PRECISION,
#         t10y DOUBLE PRECISION,
#         t2y DOUBLE PRECISION,
#         spread_10y_2y DOUBLE PRECISION,
#         unemployment DOUBLE PRECISION,
#         y1 DOUBLE PRECISION,
#         PRIMARY KEY (ticker, timestamp)
#     );
# """)

# # --- Helper per arrotondare il timestamp al minuto più vicino ---
# def round_to_minute(dt_obj):
#     return dt_obj.replace(second=0, microsecond=0)

# # --- Funzione per creare le tabelle in PostgreSQL ---
# def create_tables_if_not_exists():
#     try:
#         conn = psycopg2.connect(
#             host=POSTGRES_HOST,
#             port=POSTGRES_PORT,
#             database=POSTGRES_DB,
#             user=POSTGRES_USER,
#             password=POSTGRES_PASSWORD
#         )
#         cur = conn.cursor()
        
#         print(f"🔗 [DB] Connected to PostgreSQL. Creating tables if not exists...", file=sys.stderr)

#         cur.execute(CREATE_TABLE_QUERY_TEMPLATE.format(table_name=sql.Identifier(TABLE_STREAM1)))
#         print(f"✅ [DB] Table '{TABLE_STREAM1}' ensured.", file=sys.stderr)
        
#         cur.execute(CREATE_TABLE_QUERY_TEMPLATE.format(table_name=sql.Identifier(TABLE_STREAM2)))
#         print(f"✅ [DB] Table '{TABLE_STREAM2}' ensured.", file=sys.stderr)
        
#         conn.commit()
#         cur.close()
#         conn.close()
#     except Exception as e:
#         print(f"❌ [DB ERROR] Could not connect to PostgreSQL or create tables: {e}", file=sys.stderr)
#         sys.exit(1) # Termina lo script se non si connette al DB

# # --- CoProcessFunction per unire e processare i dati ---
# class AggregatedDataProcessor(CoProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         # Stato per i dati aggregati in attesa del loro y1
#         # La chiave è il timestamp arrotondato del dato aggregato
#         self.aggregated_data_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "aggregated_data_state",
#                 Types.STRING(), # timestamp rounded (e.g., "2023-10-27T09:30:00")
#                 Types.STRING()  # full aggregated data JSON string
#             )
#         )
        
#         # Stato per i prezzi di trade che possono essere y1
#         # La chiave è il timestamp arrotondato del trade
#         self.trade_prices_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "trade_prices_state",
#                 Types.STRING(), # timestamp rounded (e.g., "2023-10-27T09:31:00")
#                 Types.LIST(Types.FLOAT()) # lista di prezzi per quel minuto
#             )
#         )

#         # Stato per tracciare se il primo record di un minuto è già stato inviato a prova_stream1
#         # La chiave è il timestamp arrotondato del dato aggregato
#         self.first_record_sent_state = runtime_context.get_map_state(
#             MapStateDescriptor(
#                 "first_record_sent_state",
#                 Types.STRING(), # timestamp rounded
#                 Types.BOOLEAN() # True if first record sent
#             )
#         )
        
#         self.postgres_conn = None
#         self.postgres_cursor = None

#         self._connect_to_postgres()

#     def _connect_to_postgres(self):
#         try:
#             self.postgres_conn = psycopg2.connect(
#                 host=POSTGRES_HOST,
#                 port=POSTGRES_PORT,
#                 database=POSTGRES_DB,
#                 user=POSTGRES_USER,
#                 password=POSTGRES_PASSWORD
#             )
#             self.postgres_cursor = self.postgres_conn.cursor()
#             print(f"🔗 [DB] Successfully reconnected to PostgreSQL.", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [DB ERROR] Failed to connect to PostgreSQL: {e}", file=sys.stderr)
#             # In un ambiente di produzione, qui potresti voler retry o notificare un errore critico

#     def close(self):
#         if self.postgres_cursor:
#             self.postgres_cursor.close()
#         if self.postgres_conn:
#             self.postgres_conn.close()
#         print("🔗 [DB] PostgreSQL connection closed.", file=sys.stderr)

#     def _is_market_hours(self, dt_obj_ny):
#             """Checks if the given NY time object is within market hours (9:30 AM - 3:59 PM NYT) and a weekday."""
            
#             # 1. Check if it's a weekday (Monday=0, Sunday=6)
#             if dt_obj_ny.weekday() >= 7: # 5 is Saturday, 6 is Sunday
#                 return False # Not a weekday, so not market hours

#             # 2. Check if it's within daily market hours
#             market_open = dt_obj_ny.replace(hour=3, minute=30, second=0, microsecond=0)
#             market_close = dt_obj_ny.replace(hour=15, minute=59, second=0, microsecond=0)
            
#             return market_open <= dt_obj_ny < market_close

#     def process_element1(self, value: str, ctx: CoProcessFunction.Context):
#         """Processes elements from the 'aggregated-data' stream."""
#         try:
#             data = json.loads(value)
#             ticker = data.get("ticker")
#             ts_str = data.get("timestamp")
            
#             if not ticker or not ts_str:
#                 print(f"[WARN] Aggregated data missing ticker or timestamp: {value}", file=sys.stderr)
#                 return

#             timestamp_utc = isoparse(ts_str).astimezone(timezone.utc)
#             timestamp_ny = timestamp_utc.astimezone(NY_TZ)

#             if not self._is_market_hours(timestamp_ny):
#                 # print(f"[INFO] Skipping aggregated data outside market hours: {value}", file=sys.stderr)
#                 return

#             # Arrotonda il timestamp al minuto per chiave dello stato e del ground truth
#             # Questo è il timestamp del dato aggregato
#             rounded_ts_ny = round_to_minute(timestamp_ny)
            
#             # Timestamp del ground truth (y1), cioè un minuto dopo
#             y1_ts_ny = rounded_ts_ny + timedelta(minutes=1)

#             # Store the aggregated data, keyed by its *own* rounded timestamp
#             # We use the original timestamp for the state key to avoid collisions within the same minute
#             # and to allow multiple aggregated data points per minute
#             aggregated_data_key = f"{ticker}-{ts_str}" 
#             self.aggregated_data_state.put(aggregated_data_key, value)
            
#             # Register a processing time timer for when the y1 price is expected
#             # (1 minute after the *rounded* timestamp of the aggregated data)
#             timer_timestamp_ms = int(y1_ts_ny.astimezone(timezone.utc).timestamp() * 1000)
#             ctx.timer_service().register_processing_time_timer(timer_timestamp_ms)
            
#             print(f"📝 [AGG] Stored aggregated data for {ticker} @ {ts_str}. Waiting for y1 at {y1_ts_ny.isoformat()}", file=sys.stderr)

#         except json.JSONDecodeError:
#             print(f"❌ [ERROR] Failed to decode JSON from aggregated data: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [ERROR] Processing aggregated data ({value}): {e}", file=sys.stderr)

#     def process_element2(self, value: str, ctx: CoProcessFunction.Context):
#         """Processes elements from the 'stock_trades' stream."""
#         try:
#             data = json.loads(value)
#             ticker = data.get("ticker")
#             ts_str = data.get("timestamp")
#             price = data.get("price")

#             if not ticker or not ts_str or price is None:
#                 print(f"[WARN] Trade data missing ticker, timestamp or price: {value}", file=sys.stderr)
#                 return

#             timestamp_utc = isoparse(ts_str).astimezone(timezone.utc)
#             timestamp_ny = timestamp_utc.astimezone(NY_TZ)

#             if not self._is_market_hours(timestamp_ny):
#                 # print(f"[INFO] Skipping trade data outside market hours: {value}", file=sys.stderr)
#                 return
            
#             # Arrotonda il timestamp del trade al minuto
#             rounded_trade_ts_ny = round_to_minute(timestamp_ny)

#             # Store the trade price, keyed by its rounded timestamp
#             # We want to store all prices for a given minute, to later calculate an average or pick one.
#             # Using a ListState might be better, but MapState with list as value works for simplicity here.
#             current_prices = list(self.trade_prices_state.get(rounded_trade_ts_ny.isoformat()) or [])
#             current_prices.append(float(price))
#             self.trade_prices_state.put(rounded_trade_ts_ny.isoformat(), current_prices)
            
#             print(f"📈 [TRADE] Stored trade price for {ticker} @ {ts_str} (rounded: {rounded_trade_ts_ny.isoformat()})", file=sys.stderr)

#         except json.JSONDecodeError:
#             print(f"❌ [ERROR] Failed to decode JSON from trade data: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"❌ [ERROR] Processing trade data ({value}): {e}", file=sys.stderr)

#     def on_timer(self, timestamp: int, ctx: CoProcessFunction.Context):
#         """Called when a registered timer fires."""
#         ticker = ctx.get_current_key()
        
#         # The timer timestamp is in milliseconds UTC
#         timer_timestamp_utc = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
#         timer_timestamp_ny = timer_timestamp_utc.astimezone(NY_TZ)
        
#         # This timer timestamp represents the *y1_ts_ny* we set earlier,
#         # so we are looking for trades that occurred at this minute.
#         y1_expected_minute_ny = timer_timestamp_ny

#         # Check if we have trade prices for this minute (which is the y1 for previous minute's aggregated data)
#         y1_prices = list(self.trade_prices_state.get(y1_expected_minute_ny.isoformat()) or [])

#         # Iterate through all aggregated data points that are waiting for this y1
#         # The key in aggregated_data_state contains the original timestamp (ticker-ts_str)
#         # We need to filter based on whether their *y1_ts_ny* matches the current timer timestamp.
        
#         # Get all aggregated data entries for the current ticker
#         # Note: Iterating and removing from a MapState while iterating can be tricky.
#         # It's safer to build a list of keys to process and then remove.
#         keys_to_process = []
#         for agg_data_key_full in list(self.aggregated_data_state.keys()):
#             # agg_data_key_full format: "TICKER-YYYY-MM-DDTHH:MM:SS.mmmmmm+00:00"
#             parts = agg_data_key_full.split('-', 1) # Split only on the first '-'
#             if len(parts) < 2: continue # Skip malformed keys
            
#             agg_ticker = parts[0]
#             if agg_ticker != ticker: continue # Ensure it's for the current key_by ticker

#             agg_ts_str = parts[1]
#             try:
#                 agg_timestamp_utc = isoparse(agg_ts_str).astimezone(timezone.utc)
#                 agg_timestamp_ny = agg_timestamp_utc.astimezone(NY_TZ)
                
#                 # Calculate the y1 timestamp for this specific aggregated data point
#                 agg_rounded_ts_ny = round_to_minute(agg_timestamp_ny)
#                 agg_y1_ts_ny = agg_rounded_ts_ny + timedelta(minutes=1)

#                 # Check if the y1 timestamp for this aggregated data matches the timer's timestamp
#                 # We compare rounded minutes for this check.
#                 if agg_y1_ts_ny.replace(second=0, microsecond=0) == y1_expected_minute_ny.replace(second=0, microsecond=0):
#                     keys_to_process.append(agg_data_key_full)

#             except ValueError:
#                 print(f"❌ [ERROR] Invalid timestamp format in aggregated data state key: {agg_data_key_full}", file=sys.stderr)
#             except Exception as e:
#                 print(f"❌ [ERROR] Error processing aggregated data key {agg_data_key_full} for timer: {e}", file=sys.stderr)

#         # Process the found aggregated data points
#         if keys_to_process:
#             for agg_data_key_full in keys_to_process:
#                 agg_data_json_str = self.aggregated_data_state.get(agg_data_key_full)
#                 if not agg_data_json_str:
#                     continue # Should not happen, but for safety

#                 agg_data = json.loads(agg_data_json_str)
#                 original_agg_ts_str = agg_data.get("timestamp") # Original timestamp from the aggregated data

#                 # Calculate the y1 value (e.g., mean of prices in that minute)
#                 y1_value = None
#                 if y1_prices:
#                     y1_value = sum(y1_prices) / len(y1_prices)
                
#                 # Add y1 to the aggregated data
#                 agg_data["y1"] = y1_value

#                 # Determine which table to write to (prova_stream1 or prova_stream2)
#                 # Use the rounded timestamp of the *aggregated data* for the first_record_sent_state check
#                 agg_timestamp_utc = isoparse(original_agg_ts_str).astimezone(timezone.utc)
#                 agg_timestamp_ny = agg_timestamp_utc.astimezone(NY_TZ)
#                 agg_rounded_ts_ny = round_to_minute(agg_timestamp_ny)
                
#                 table_to_insert = TABLE_STREAM2 # Default to stream2
#                 first_record_key = f"{ticker}-{agg_rounded_ts_ny.isoformat()}"

#                 if not self.first_record_sent_state.contains(first_record_key) or not self.first_record_sent_state.get(first_record_key):
#                     table_to_insert = TABLE_STREAM1
#                     self.first_record_sent_state.put(first_record_key, True) # Mark as sent

#                 self._write_to_postgres(agg_data, table_to_insert)

#                 # Remove the processed aggregated data from state
#                 self.aggregated_data_state.remove(agg_data_key_full)
#                 print(f"🧹 [STATE] Removed processed aggregated data for {ticker} from state.", file=sys.stderr)

#         # Clear trade prices for this specific minute once they have been used
#         # We can clear trade prices only if ALL aggregated data points for the *previous minute* have been processed.
#         # This is complex because different aggregated data points might have different y1 targets.
#         # For simplicity, we'll clear trade prices for the *timer's minute* after we've used them.
#         # This assumes that all relevant aggregated data for `y1_expected_minute_ny` has already been handled by this timer or earlier.
#         self.trade_prices_state.remove(y1_expected_minute_ny.isoformat())
#         print(f"🧹 [STATE] Cleared trade prices for {ticker} @ {y1_expected_minute_ny.isoformat()}", file=sys.stderr)


#         # Periodically clean up first_record_sent_state to avoid unbounded growth
#         # We can clear entries that are more than, say, 24 hours old.
#         cleanup_threshold_ny = datetime.now(NY_TZ) - timedelta(days=1)
#         keys_to_remove_first_record = []
#         for k in list(self.first_record_sent_state.keys()):
#             try:
#                 # Key format: "TICKER-YYYY-MM-DDTHH:MM:SS"
#                 parts = k.split('-', 1)
#                 if len(parts) < 2: continue
#                 ts_part = parts[1] # The rounded timestamp string
#                 dt_obj_ny = isoparse(ts_part).astimezone(NY_TZ)
#                 if dt_obj_ny < cleanup_threshold_ny:
#                     keys_to_remove_first_record.append(k)
#             except Exception:
#                 keys_to_remove_first_record.append(k) # Remove malformed/unparseable keys

#         for k_remove in keys_to_remove_first_record:
#             self.first_record_sent_state.remove(k_remove)
#             # print(f"🧹 [STATE] Cleaned up first_record_sent_state for {k_remove}", file=sys.stderr)

#     def _write_to_postgres(self, data: dict, table_name: str):
#         if not self.postgres_conn or self.postgres_conn.closed:
#             self._connect_to_postgres() # Attempt to reconnect if connection is lost

#         if not self.postgres_conn or self.postgres_postgres_cursor: # Double check after reconnection attempt
#             print(f"❌ [DB ERROR] No active PostgreSQL connection for writing to {table_name}.", file=sys.stderr)
#             return

#         try:
#             # Assicurati che tutti i campi esistano o siano None
#             values = {
#                 "ticker": data.get("ticker"),
#                 "timestamp": isoparse(data["timestamp"]).astimezone(timezone.utc), # Assicurati che sia in UTC per PostgreSQL
#                 "price_mean_1min": data.get("price_mean_1min"),
#                 "price_mean_5min": data.get("price_mean_5min"),
#                 "price_std_5min": data.get("price_std_5min"),
#                 "price_mean_30min": data.get("price_mean_30min"),
#                 "price_std_30min": data.get("price_std_30min"),
#                 "size_tot_1min": data.get("size_tot_1min"),
#                 "size_tot_5min": data.get("size_tot_5min"),
#                 "size_tot_30min": data.get("size_tot_30min"),
#                 "sentiment_bluesky_mean_2hours": data.get("sentiment_bluesky_mean_2h"), # Nome campo corretto
#                 "sentiment_bluesky_mean_1day": data.get("sentiment_bluesky_mean_1d"),
#                 "sentiment_news_mean_1day": data.get("sentiment_news_mean_1d"),
#                 "sentiment_news_mean_3days": data.get("sentiment_news_mean_3d"),
#                 "sentiment_general_bluesky_mean_2hours": data.get("sentiment_bluesky_mean_general_2hours"),
#                 "sentiment_general_bluesky_mean_1day": data.get("sentiment_bluesky_mean_general_1d"),
#                 "minutes_since_open": data.get("minutes_since_open"),
#                 "day_of_week": data.get("day_of_week"),
#                 "day_of_month": data.get("day_of_month"),
#                 "week_of_year": data.get("week_of_year"),
#                 "month_of_year": data.get("month_of_year"),
#                 "market_open_spike_flag": data.get("market_open_spike_flag"),
#                 "market_close_spike_flag": data.get("market_close_spike_flag"),
#                 "eps": data.get("eps"),
#                 "free_cash_flow": data.get("freeCashFlow"), # Nome campo corretto
#                 "profit_margin": data.get("profit_margin"),
#                 "debt_to_equity": data.get("debt_to_equity"),
#                 "gdp_real": data.get("gdp_real"),
#                 "cpi": data.get("cpi"),
#                 "ffr": data.get("ffr"),
#                 "t10y": data.get("t10y"),
#                 "t2y": data.get("t2y"),
#                 "spread_10y_2y": data.get("spread_10y_2y"),
#                 "unemployment": data.get("unemployment"),
#                 "y1": data.get("y1") # Il valore y1 calcolato
#             }
            
#             # Ordina le chiavi per essere coerenti con la query SQL
#             columns = [
#                 "ticker", "timestamp", "price_mean_1min", "price_mean_5min",
#                 "price_std_5min", "price_mean_30min", "price_std_30min",
#                 "size_tot_1min", "size_tot_5min", "size_tot_30min",
#                 "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
#                 "sentiment_news_mean_1day", "sentiment_news_mean_3days",
#                 "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
#                 "minutes_since_open", "day_of_week", "day_of_month", "week_of_year",
#                 "month_of_year", "market_open_spike_flag", "market_close_spike_flag",
#                 "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
#                 "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
#                 "y1"
#             ]
            
#             # Mappa i valori in base all'ordine delle colonne
#             ordered_values = tuple(values.get(col) for col in columns)

#             insert_query = sql.SQL("""
#                 INSERT INTO {table_name} ({columns})
#                 VALUES ({values})
#                 ON CONFLICT (ticker, timestamp) DO UPDATE SET
#                     price_mean_1min = EXCLUDED.price_mean_1min,
#                     price_mean_5min = EXCLUDED.price_mean_5min,
#                     price_std_5min = EXCLUDED.price_std_5min,
#                     price_mean_30min = EXCLUDED.price_mean_30min,
#                     price_std_30min = EXCLUDED.price_std_30min,
#                     size_tot_1min = EXCLUDED.size_tot_1min,
#                     size_tot_5min = EXCLUDED.size_tot_5min,
#                     size_tot_30min = EXCLUDED.size_tot_30min,
#                     sentiment_bluesky_mean_2hours = EXCLUDED.sentiment_bluesky_mean_2hours,
#                     sentiment_bluesky_mean_1day = EXCLUDED.sentiment_bluesky_mean_1day,
#                     sentiment_news_mean_1day = EXCLUDED.sentiment_news_mean_1day,
#                     sentiment_news_mean_3days = EXCLUDED.sentiment_news_mean_3days,
#                     sentiment_general_bluesky_mean_2hours = EXCLUDED.sentiment_general_bluesky_mean_2hours,
#                     sentiment_general_bluesky_mean_1day = EXCLUDED.sentiment_general_bluesky_mean_1day,
#                     minutes_since_open = EXCLUDED.minutes_since_open,
#                     day_of_week = EXCLUDED.day_of_week,
#                     day_of_month = EXCLUDED.day_of_month,
#                     week_of_year = EXCLUDED.week_of_year,
#                     month_of_year = EXCLUDED.month_of_year,
#                     market_open_spike_flag = EXCLUDED.market_open_spike_flag,
#                     market_close_spike_flag = EXCLUDED.market_close_spike_flag,
#                     eps = EXCLUDED.eps,
#                     free_cash_flow = EXCLUDED.free_cash_flow,
#                     profit_margin = EXCLUDED.profit_margin,
#                     debt_to_equity = EXCLUDED.debt_to_equity,
#                     gdp_real = EXCLUDED.gdp_real,
#                     cpi = EXCLUDED.cpi,
#                     ffr = EXCLUDED.ffr,
#                     t10y = EXCLUDED.t10y,
#                     t2y = EXCLUDED.t2y,
#                     spread_10y_2y = EXCLUDED.spread_10y_2y,
#                     unemployment = EXCLUDED.unemployment,
#                     y1 = EXCLUDED.y1
#             """).format(
#                 table_name=sql.Identifier(table_name),
#                 columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
#                 values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
#             )
            
#             self.postgres_cursor.execute(insert_query, ordered_values)
#             self.postgres_conn.commit()
#             print(f"💾 [DB] Successfully inserted/updated data for {data.get('ticker')} @ {data.get('timestamp')} into {table_name}", file=sys.stderr)

#         except psycopg2.Error as db_error:
#             self.postgres_conn.rollback() # Rollback in case of error
#             print(f"❌ [DB ERROR] Database error writing to {table_name} for {data.get('ticker')}: {db_error}", file=sys.stderr)
#             # Potresti voler riconnetterti in caso di errori specifici di connessione persa
#             if "connection" in str(db_error).lower():
#                 self._connect_to_postgres()
#         except Exception as e:
#             print(f"❌ [DB ERROR] General error writing to {table_name} for {data.get('ticker')}: {e}", file=sys.stderr)


# def main():
#     # Assicurati che le tabelle esistano prima di avviare il job Flink
#     create_tables_if_not_exists()

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1) # Mantieni a 1 per semplicità e per la gestione di global state implicita (anche se per questo job non è un problema)

#     # Consumers per i due topic Kafka
#     aggregated_consumer = FlinkKafkaConsumer(
#         topics=[AGGREGATED_DATA_TOPIC],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#             'group.id': KAFKA_GROUP_ID + "_agg",
#             'auto.offset.reset': 'earliest'
#         }
#     )

#     trades_consumer = FlinkKafkaConsumer(
#         topics=[STOCK_TRADES_TOPIC],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#             'group.id': KAFKA_GROUP_ID + "_trades",
#             'auto.offset.reset': 'earliest'
#         }
#     )

#     agg_stream = env.add_source(aggregated_consumer, type_info=Types.STRING())
#     trades_stream = env.add_source(trades_consumer, type_info=Types.STRING())

#     # Key streams by ticker for co-processing
#     # Important: The key for trades must be just the ticker
#     keyed_agg_stream = agg_stream.key_by(
#         lambda x: json.loads(x).get("ticker"),
#         key_type=Types.STRING()
#     )
#     keyed_trades_stream = trades_stream.key_by(
#         lambda x: json.loads(x).get("ticker"),
#         key_type=Types.STRING()
#     )

#     # CoProcess i due stream
#     # No output type is explicitly needed as we are writing to an external sink
#     keyed_agg_stream.connect(keyed_trades_stream) \
#         .process(AggregatedDataProcessor())

#     env.execute("Flink PostgreSQL Sink Job")

# if __name__ == "__main__":
#     main()
