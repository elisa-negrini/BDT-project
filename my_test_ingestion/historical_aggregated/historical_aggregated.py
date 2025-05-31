# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from datetime import datetime, timedelta, time as dtime
# from zoneinfo import ZoneInfo
# from minio import Minio
# from io import BytesIO
# import json
# import numpy as np
# import pandas as pd
# import os
# import sys

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# macro_data_by_series = {}
# fundamentals_by_symbol_year = {}
# pending_stock_batches = []
# ready_flags = {"macro_ready": False, "fundamentals_ready": False}

# def get_macro_values_for_date(date_obj):
#     result = {}
#     for series, entries in macro_data_by_series.items():
#         selected_value = None
#         for entry_date, value in sorted(entries):
#             if entry_date <= date_obj:
#                 selected_value = value
#             else:
#                 break
#         if selected_value is not None:
#             result[series] = selected_value
#     return result

# def get_fundamentals(symbol, year):
#     return fundamentals_by_symbol_year.get((symbol, year), {})

# class FullDayAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         self.bucket_name = "aggregated-data"
#         self.minio_client = Minio(
#             endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
#             access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
#             secret_key=os.getenv("MINIO_SECRET_KEY", "admin123"),
#             secure=False
#         )
#         if not self.minio_client.bucket_exists(self.bucket_name):
#             self.minio_client.make_bucket(self.bucket_name)

#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)

#             if "series" in message and "value" in message and "date" in message:
#                 try:
#                     series = message["series"]
#                     date = datetime.fromisoformat(message["date"]).date()
#                     val = float(message["value"])
#                     macro_data_by_series.setdefault(series, []).append((date, val))
#                     macro_data_by_series[series].sort()
#                     ready_flags["macro_ready"] = True
#                 except:
#                     pass
#                 return

#             if "symbol" in message and "calendarYear" in message:
#                 try:
#                     symbol = message["symbol"]
#                     year = int(message["calendarYear"])
#                     fundamentals_by_symbol_year[(symbol, year)] = {
#                         "eps": message.get("eps"),
#                         "freeCashFlow": message.get("cashflow_freeCashFlow"),
#                         "revenue": message.get("revenue"),
#                         "netIncome": message.get("netIncome"),
#                         "balance_totalDebt": message.get("balance_totalDebt"),
#                         "balance_totalStockholdersEquity": message.get("balance_totalStockholdersEquity")
#                     }
#                     ready_flags["fundamentals_ready"] = True
#                 except:
#                     pass
#                 return

#             if ready_flags["macro_ready"] and ready_flags["fundamentals_ready"] and pending_stock_batches:
#                 for buffered_value in pending_stock_batches:
#                     yield from self.process_element(buffered_value, ctx)
#                 pending_stock_batches.clear()

#             if not (ready_flags["macro_ready"] and ready_flags["fundamentals_ready"]):
#                 pending_stock_batches.append(value)
#                 return

#             ticker = message.get("ticker")
#             if ticker not in TOP_30_TICKERS:
#                 return

#             rows = message.get("data", [])
#             parsed = []
#             for entry in rows:
#                 try:
#                     parsed.append({
#                         "symbol": ticker,
#                         "timestamp": datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00")),
#                         "open": float(entry["open"]),
#                         "close": float(entry["close"]),
#                         "volume": float(entry["volume"]),
#                     })
#                 except:
#                     continue

#             parsed.sort(key=lambda x: x["timestamp"])
#             batch_results = []

#             for i in range(len(parsed)):
#                 row = parsed[i]
#                 now = row["timestamp"]

#                 def window_vals(field, minutes):
#                     ts_limit = now - timedelta(minutes=minutes)
#                     return [p[field] for p in parsed if ts_limit <= p["timestamp"] <= now]

#                 def mean(values): return float(np.mean(values)) if values else None
#                 def std(values): return float(np.std(values)) if values else None
#                 def total(values): return float(np.sum(values)) if values else 0.0

#                 macro_values = get_macro_values_for_date(now.date())
#                 fundamentals = get_fundamentals(row["symbol"], now.year - 1)

#                 profit_margin = None
#                 if fundamentals.get("revenue") not in [None, 0]:
#                     profit_margin = fundamentals["netIncome"] / fundamentals["revenue"]

#                 debt_to_equity = None
#                 if fundamentals.get("balance_totalStockholdersEquity") not in [None, 0]:
#                     debt_to_equity = fundamentals["balance_totalDebt"] / fundamentals["balance_totalStockholdersEquity"]

#                 local_time = now.astimezone(ZoneInfo("America/New_York"))
#                 market_open_flag = int(dtime(9, 30) <= local_time.time() <= dtime(9, 34))
#                 market_close_flag = int(dtime(15, 56) <= local_time.time() <= dtime(16, 0))

#                 result = {
#                     "ticker": row["symbol"],
#                     "timestamp": now.isoformat(),
#                     "price_mean_1min": mean(window_vals("open", 1)),
#                     "price_mean_5min": mean(window_vals("open", 5)),
#                     "price_std_5min": std(window_vals("open", 5)),
#                     "price_mean_30min": mean(window_vals("open", 30)),
#                     "price_std_30min": std(window_vals("open", 30)),
#                     "size_tot_1min": total(window_vals("volume", 1)),
#                     "size_tot_5min": total(window_vals("volume", 5)),
#                     "size_tot_30min": total(window_vals("volume", 30)),
#                     "sentiment_bluesky_mean_2hours": 0.0,
#                     "sentiment_bluesky_mean_1day": 0.0,
#                     "sentiment_news_mean_1day": 0.0,
#                     "sentiment_news_mean_3days": 0.0,
#                     "sentiment_general_bluesky_mean_2hours": 0.0,
#                     "sentiment_general_bluesky_mean_1day": 0.0,
#                     "minutes_since_open": (now - now.replace(hour=13, minute=30)).total_seconds() // 60,
#                     "day_of_week": now.weekday(),
#                     "day_of_month": now.day,
#                     "week_of_year": now.isocalendar()[1],
#                     "month_of_year": now.month,
#                     "market_open_spike_flag": market_open_flag,
#                     "market_close_spike_flag": market_close_flag,
#                     "eps": fundamentals.get("eps"),
#                     "freeCashFlow": fundamentals.get("freeCashFlow"),
#                     "profit_margin": profit_margin,
#                     "debt_to_equity": debt_to_equity
#                 }

#                 result.update(macro_values)
#                 result["y1"] = row["close"]
#                 if i <= len(parsed) - 5:
#                     result["y5"] = parsed[i + 4]["close"]

#                 batch_results.append(result)

#             # Scrittura su MinIO
#             if batch_results:
#                 df = pd.DataFrame(batch_results)
#                 df["timestamp"] = pd.to_datetime(df["timestamp"])
#                 df["date"] = df["timestamp"].dt.date

#                 for (symbol, day), group in df.groupby(["ticker", "date"]):
#                     year = day.year
#                     month = f"{day.month:02d}"
#                     day_str = f"{day.day:02d}"
#                     parquet_buffer = BytesIO()
#                     group.drop(columns="date").to_parquet(parquet_buffer, index=False)
#                     parquet_buffer.seek(0)

#                     object_name = f"{symbol}/{year}/{month}/{day_str}.parquet"
#                     self.minio_client.put_object(
#                         self.bucket_name,
#                         object_name,
#                         parquet_buffer,
#                         length=parquet_buffer.getbuffer().nbytes,
#                         content_type="application/octet-stream"
#                     )

#         except Exception as e:
#             print(f"[ERROR] Processing element: {e}", file=sys.stderr)

# def extract_key(json_str):
#     try:
#         data = json.loads(json_str)
#         return data.get("ticker") or data.get("symbol") or data.get("series") or "unknown"
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     consumer = FlinkKafkaConsumer(
#         topics=["h_alpaca", "h_macrodata", "h_company"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
#             "group.id": "flink_batch_group",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(extract_key, key_type=Types.STRING())
#     processed = keyed.process(FullDayAggregator(), output_type=Types.STRING())

#     env.execute("Historical Aggregation to MinIO")

# if __name__ == "__main__":
#     main()


















# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from datetime import datetime, timedelta, time as dtime
# from zoneinfo import ZoneInfo
# import json
# import numpy as np
# import pandas as pd
# import os
# import sys
# import psycopg2
# from psycopg2 import sql

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# macro_data_by_series = {}
# fundamentals_by_symbol_year = {}
# pending_stock_batches = []
# ready_flags = {"macro_ready": False, "fundamentals_ready": False}

# def get_macro_values_for_date(date_obj):
#     result = {}
#     for series, entries in macro_data_by_series.items():
#         selected_value = None
#         for entry_date, value in sorted(entries):
#             if entry_date <= date_obj:
#                 selected_value = value
#             else:
#                 break
#         if selected_value is not None:
#             result[series] = selected_value
#     return result

# def get_fundamentals(symbol, year):
#     return fundamentals_by_symbol_year.get((symbol, year), {})

# class FullDayAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         self.db_name = "aggregated-data"
#         self.db_user = "admin"
#         self.db_password = "admin123"
#         # Use environment variables for host and port, with sensible defaults for local development
#         self.db_host = os.getenv("POSTGRES_HOST", "localhost")
#         self.db_port = os.getenv("POSTGRES_PORT", "5432")

#         self.conn = None
#         self.cursor = None
#         self._connect_to_db()

#     def _connect_to_db(self):
#         try:
#             self.conn = psycopg2.connect(
#                 dbname=self.db_name,
#                 user=self.db_user,
#                 password=self.db_password,
#                 host=self.db_host,
#                 port=self.db_port
#             )
#             self.cursor = self.conn.cursor()
#             print("[INFO] Successfully connected to PostgreSQL.")
#             self._create_table_if_not_exists()
#         except Exception as e:
#             print(f"[ERROR] Could not connect to PostgreSQL: {e}", file=sys.stderr)
#             # In a real Flink job, you might want to throw an exception to fail the task
#             # sys.exit(1) # This would kill the Python process, not ideal for Flink task failure
#             raise RuntimeError(f"Failed to connect to PostgreSQL: {e}") # Raise a Flink-compatible error

#     def _create_table_if_not_exists(self):
#         table_name = "daily_stock_aggregations"
#         create_table_query = sql.SQL("""
#             CREATE TABLE IF NOT EXISTS {table_name} (
#                 ticker VARCHAR(10) NOT NULL,
#                 timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
#                 price_mean_1min DOUBLE PRECISION,
#                 price_mean_5min DOUBLE PRECISION,
#                 price_std_5min DOUBLE PRECISION,
#                 price_mean_30min DOUBLE PRECISION,
#                 price_std_30min DOUBLE PRECISION,
#                 size_tot_1min DOUBLE PRECISION,
#                 size_tot_5min DOUBLE PRECISION,
#                 size_tot_30min DOUBLE PRECISION,
#                 sentiment_bluesky_mean_2hours DOUBLE PRECISION,
#                 sentiment_bluesky_mean_1day DOUBLE PRECISION,
#                 sentiment_news_mean_1day DOUBLE PRECISION,
#                 sentiment_news_mean_3days DOUBLE PRECISION,
#                 sentiment_general_bluesky_mean_2hours DOUBLE PRECISION,
#                 sentiment_general_bluesky_mean_1day DOUBLE PRECISION,
#                 minutes_since_open DOUBLE PRECISION,
#                 day_of_week INTEGER,
#                 day_of_month INTEGER,
#                 week_of_year INTEGER,
#                 month_of_year INTEGER,
#                 market_open_spike_flag INTEGER,
#                 market_close_spike_flag INTEGER,
#                 eps DOUBLE PRECISION,
#                 freeCashFlow DOUBLE PRECISION,
#                 profit_margin DOUBLE PRECISION,
#                 debt_to_equity DOUBLE PRECISION,
#                 -- Macroeconomic data columns
#                 gdp_real DOUBLE PRECISION,
#                 cpi DOUBLE PRECISION,
#                 ffr DOUBLE PRECISION,
#                 t10y DOUBLE PRECISION,
#                 t2y DOUBLE PRECISION,
#                 spread_10y_2y DOUBLE PRECISION,
#                 unemployment DOUBLE PRECISION,
#                 y1 DOUBLE PRECISION,
#                 y5 DOUBLE PRECISION,
#                 PRIMARY KEY (ticker, timestamp)
#             );
#         """).format(table_name=sql.Identifier(table_name))
#         try:
#             self.cursor.execute(create_table_query)
#             self.conn.commit()
#             print(f"[INFO] Table '{table_name}' checked/created successfully.")
#         except Exception as e:
#             print(f"[ERROR] Could not create table {table_name}: {e}", file=sys.stderr)
#             self.conn.rollback()
#             raise # Re-raise the exception to inform Flink about the failure


#     def close(self):
#         if self.cursor:
#             self.cursor.close()
#         if self.conn:
#             self.conn.close()
#         print("[INFO] PostgreSQL connection closed.")

#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)

#             # Handle Macro Data
#             if "series" in message and "value" in message and "date" in message:
#                 try:
#                     series = message["series"]
#                     date = datetime.fromisoformat(message["date"]).date()
#                     val = float(message["value"])
#                     macro_data_by_series.setdefault(series, []).append((date, val))
#                     macro_data_by_series[series].sort()
#                     # Check if all required macro series are present (optional, but good for readiness)
#                     required_macro_series = ["gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"]
#                     if all(s in macro_data_by_series for s in required_macro_series) and \
#                        all(macro_data_by_series[s] for s in required_macro_series): # ensure they have at least one entry
#                         ready_flags["macro_ready"] = True
#                 except Exception as e:
#                     print(f"[ERROR] Error processing macro data: {e} - Message: {message}", file=sys.stderr)
#                 return

#             # Handle Fundamental Data
#             if "symbol" in message and "calendarYear" in message:
#                 try:
#                     symbol = message["symbol"]
#                     year = int(message["calendarYear"])
#                     fundamentals_by_symbol_year[(symbol, year)] = {
#                         "eps": message.get("eps"),
#                         "freeCashFlow": message.get("cashflow_freeCashFlow"),
#                         "revenue": message.get("revenue"),
#                         "netIncome": message.get("netIncome"),
#                         "balance_totalDebt": message.get("balance_totalDebt"),
#                         "balance_totalStockholdersEquity": message.get("balance_totalStockholdersEquity")
#                     }
#                     ready_flags["fundamentals_ready"] = True
#                 except Exception as e:
#                     print(f"[ERROR] Error processing fundamental data: {e} - Message: {message}", file=sys.stderr)
#                 return

#             # Process buffered stock data if all dependencies are ready
#             if ready_flags["macro_ready"] and ready_flags["fundamentals_ready"] and pending_stock_batches:
#                 print(f"[INFO] Processing {len(pending_stock_batches)} buffered stock batches.", file=sys.stdout)
#                 for buffered_value in list(pending_stock_batches): # Iterate over a copy as we clear it
#                     # Recursively call process_element for buffered items
#                     # yield from self.process_element(buffered_value, ctx) # This would re-buffer if not ready, better to just process
#                     self._process_stock_message(json.loads(buffered_value))
#                 pending_stock_batches.clear()

#             # Buffer stock data if dependencies are not ready
#             if not (ready_flags["macro_ready"] and ready_flags["fundamentals_ready"]):
#                 # Only buffer if it's a stock message. This prevents buffering macro/fundamental messages repeatedly.
#                 if "ticker" in message and "data" in message:
#                     pending_stock_batches.append(value)
#                     # print(f"[INFO] Buffered stock data for {message.get('ticker')}. Total buffered: {len(pending_stock_batches)}", file=sys.stdout)
#                 return
            
#             # If it's a stock message and dependencies are ready, process it directly
#             if "ticker" in message and "data" in message:
#                 self._process_stock_message(message)

#         except Exception as e:
#             print(f"[ERROR] Global error in process_element: {e} - Original Value: {value}", file=sys.stderr)
#             # Potentially re-raise if you want the Flink task to fail on any unhandled error
#             # raise

#     def _process_stock_message(self, message):
#         ticker = message.get("ticker")
#         if ticker not in TOP_30_TICKERS:
#             return

#         rows = message.get("data", [])
#         parsed = []
#         for entry in rows:
#             try:
#                 parsed.append({
#                     "symbol": ticker,
#                     "timestamp": datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00")),
#                     "open": float(entry["open"]),
#                     "close": float(entry["close"]),
#                     "volume": float(entry["volume"]),
#                 })
#             except Exception as e:
#                 print(f"[ERROR] Error parsing stock entry for {ticker}: {e} - Entry: {entry}", file=sys.stderr)
#                 continue

#         parsed.sort(key=lambda x: x["timestamp"])
#         batch_results = []

#         for i in range(len(parsed)):
#             row = parsed[i]
#             now = row["timestamp"]

#             def window_vals(field, minutes):
#                 ts_limit = now - timedelta(minutes=minutes)
#                 # Filter for entries within the window, including the current one
#                 return [p[field] for p in parsed if ts_limit <= p["timestamp"] <= now]

#             def mean(values): return float(np.mean(values)) if values else None
#             def std(values): return float(np.std(values)) if values else None
#             def total(values): return float(np.sum(values)) if values else 0.0

#             macro_values = get_macro_values_for_date(now.date())
#             fundamentals = get_fundamentals(row["symbol"], now.year - 1)

#             profit_margin = None
#             if fundamentals.get("revenue") not in [None, 0]:
#                 profit_margin = fundamentals["netIncome"] / fundamentals["revenue"]

#             debt_to_equity = None
#             if fundamentals.get("balance_totalStockholdersEquity") not in [None, 0]:
#                 debt_to_equity = fundamentals["balance_totalDebt"] / fundamentals["balance_totalStockholdersEquity"]

#             local_time = now.astimezone(ZoneInfo("America/New_York"))
#             market_open_flag = int(dtime(9, 30) <= local_time.time() <= dtime(9, 34))
#             market_close_flag = int(dtime(15, 56) <= local_time.time() <= dtime(16, 0))

#             result = {
#                 "ticker": row["symbol"],
#                 "timestamp": now.isoformat(), # Store as ISO format string, convert to TIMESTAMP in DB
#                 "price_mean_1min": mean(window_vals("open", 1)),
#                 "price_mean_5min": mean(window_vals("open", 5)),
#                 "price_std_5min": std(window_vals("open", 5)),
#                 "price_mean_30min": mean(window_vals("open", 30)),
#                 "price_std_30min": std(window_vals("open", 30)),
#                 "size_tot_1min": total(window_vals("volume", 1)),
#                 "size_tot_5min": total(window_vals("volume", 5)),
#                 "size_tot_30min": total(window_vals("volume", 30)),
#                 "sentiment_bluesky_mean_2hours": 0.0, # Placeholder
#                 "sentiment_bluesky_mean_1day": 0.0,  # Placeholder
#                 "sentiment_news_mean_1day": 0.0,      # Placeholder
#                 "sentiment_news_mean_3days": 0.0,     # Placeholder
#                 "sentiment_general_bluesky_mean_2hours": 0.0, # Placeholder
#                 "sentiment_general_bluesky_mean_1day": 0.0,   # Placeholder
#                 "minutes_since_open": (local_time - local_time.replace(hour=9, minute=30, second=0, microsecond=0)).total_seconds() // 60,
#                 "day_of_week": now.weekday(),
#                 "day_of_month": now.day,
#                 "week_of_year": now.isocalendar()[1],
#                 "month_of_year": now.month,
#                 "market_open_spike_flag": market_open_flag,
#                 "market_close_spike_flag": market_close_flag,
#                 "eps": fundamentals.get("eps"),
#                 "freeCashFlow": fundamentals.get("freeCashFlow"),
#                 "profit_margin": profit_margin,
#                 "debt_to_equity": debt_to_equity
#             }

#             # Add specific macro values using the new series names
#             result["gdp_real"] = macro_values.get("gdp_real", None)
#             result["cpi"] = macro_values.get("cpi", None)
#             result["ffr"] = macro_values.get("ffr", None)
#             result["t10y"] = macro_values.get("t10y", None)
#             result["t2y"] = macro_values.get("t2y", None)
#             result["spread_10y_2y"] = macro_values.get("spread_10y_2y", None)
#             result["unemployment"] = macro_values.get("unemployment", None)

#             result["y1"] = row["close"]
#             if i <= len(parsed) - 5:
#                 result["y5"] = parsed[i + 4]["close"]
#             else:
#                 result["y5"] = None # Ensure y5 is set to None if no future data exists

#             batch_results.append(result)

#         # Insert data into PostgreSQL
#         if batch_results:
#             self._insert_data_to_postgresql(batch_results)

#     def _insert_data_to_postgresql(self, data):
#         table_name = "daily_stock_aggregations"
#         columns = [
#             "ticker", "timestamp", "price_mean_1min", "price_mean_5min", "price_std_5min",
#             "price_mean_30min", "price_std_30min", "size_tot_1min", "size_tot_5min",
#             "size_tot_30min", "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
#             "sentiment_news_mean_1day", "sentiment_news_mean_3days",
#             "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
#             "minutes_since_open", "day_of_week", "day_of_month", "week_of_year",
#             "month_of_year", "market_open_spike_flag", "market_close_spike_flag",
#             "eps", "freeCashFlow", "profit_margin", "debt_to_equity",
#             "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
#             "y1", "y5"
#         ]
        
#         # Construct the VALUES part of the query with placeholders for each column
#         values_placeholder = sql.SQL(', ').join(sql.Placeholder() * len(columns))
        
#         insert_query = sql.SQL(
#             "INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder}) "
#             "ON CONFLICT (ticker, timestamp) DO UPDATE SET "
#             "price_mean_1min = EXCLUDED.price_mean_1min, "
#             "price_mean_5min = EXCLUDED.price_mean_5min, "
#             "price_std_5min = EXCLUDED.price_std_5min, "
#             "price_mean_30min = EXCLUDED.price_mean_30min, "
#             "price_std_30min = EXCLUDED.price_std_30min, "
#             "size_tot_1min = EXCLUDED.size_tot_1min, "
#             "size_tot_5min = EXCLUDED.size_tot_5min, "
#             "size_tot_30min = EXCLUDED.size_tot_30min, "
#             "sentiment_bluesky_mean_2hours = EXCLUDED.sentiment_bluesky_mean_2hours, "
#             "sentiment_bluesky_mean_1day = EXCLUDED.sentiment_bluesky_mean_1day, "
#             "sentiment_news_mean_1day = EXCLUDED.sentiment_news_mean_1day, "
#             "sentiment_news_mean_3days = EXCLUDED.sentiment_news_mean_3days, "
#             "sentiment_general_bluesky_mean_2hours = EXCLUDED.sentiment_general_bluesky_mean_2hours, "
#             "sentiment_general_bluesky_mean_1day = EXCLUDED.sentiment_general_bluesky_mean_1day, "
#             "minutes_since_open = EXCLUDED.minutes_since_open, "
#             "day_of_week = EXCLUDED.day_of_week, "
#             "day_of_month = EXCLUDED.day_of_month, "
#             "week_of_year = EXCLUDED.week_of_year, "
#             "month_of_year = EXCLUDED.month_of_year, "
#             "market_open_spike_flag = EXCLUDED.market_open_spike_flag, "
#             "market_close_spike_flag = EXCLUDED.market_close_spike_flag, "
#             "eps = EXCLUDED.eps, "
#             "freeCashFlow = EXCLUDED.freeCashFlow, "
#             "profit_margin = EXCLUDED.profit_margin, "
#             "debt_to_equity = EXCLUDED.debt_to_equity, "
#             "gdp_real = EXCLUDED.gdp_real, " # Updated macro columns
#             "cpi = EXCLUDED.cpi, "
#             "ffr = EXCLUDED.ffr, "
#             "t10y = EXCLUDED.t10y, "
#             "t2y = EXCLUDED.t2y, "
#             "spread_10y_2y = EXCLUDED.spread_10y_2y, "
#             "unemployment = EXCLUDED.unemployment, "
#             "y1 = EXCLUDED.y1, "
#             "y5 = EXCLUDED.y5"
#         ).format(
#             table_name=sql.Identifier(table_name),
#             columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
#             values_placeholder=values_placeholder
#         )

#         try:
#             records_to_insert = []
#             for record in data:
#                 # Convert timestamp string back to datetime object for psycopg2
#                 record_copy = record.copy()
#                 record_copy["timestamp"] = datetime.fromisoformat(record_copy["timestamp"])
                
#                 # Map the record dictionary to the ordered list of column values
#                 row_values = [record_copy.get(col) for col in columns]
#                 records_to_insert.append(row_values)

#             # Print the first row for verification
#             if records_to_insert:
#                 print(f"[INFO] First row of batch for insertion: {records_to_insert[0]}", file=sys.stdout)

#             self.cursor.executemany(insert_query, records_to_insert)
#             self.conn.commit()
#             print(f"[INFO] Inserted {len(data)} records into {table_name}.", file=sys.stdout)
#         except Exception as e:
#             print(f"[ERROR] Error inserting data into PostgreSQL: {e}", file=sys.stderr)
#             self.conn.rollback()


# def extract_key(json_str):
#     try:
#         data = json.loads(json_str)
#         # Prioritize ticker for stock data, then symbol for fundamentals, then series for macro
#         return data.get("ticker") or data.get("symbol") or data.get("series") or "unknown"
#     except json.JSONDecodeError:
#         print(f"[ERROR] JSON decoding failed for: {json_str[:100]}...", file=sys.stderr)
#         return "unknown"
#     except Exception as e:
#         print(f"[ERROR] Unexpected error in extract_key: {e} - String: {json_str[:100]}...", file=sys.stderr)
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1) # Keep parallelism at 1 to ensure global macro/fundamentals state is consistent across subtasks

#     consumer = FlinkKafkaConsumer(
#         topics=["h_alpaca", "h_macrodata", "h_company"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
#             "group.id": "flink_batch_group",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(extract_key, key_type=Types.STRING())
#     processed = keyed.process(FullDayAggregator(), output_type=Types.ROW([])) # VOID as we're sinking directly

#     env.execute("Historical Aggregation to PostgreSQL with Macro Data")

# if __name__ == "__main__":
#     main()







































from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
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

TOP_30_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

macro_data_by_series = {}
fundamentals_by_symbol_year = {}
pending_stock_batches = []
ready_flags = {"macro_ready": False, "fundamentals_ready": False}

def get_macro_values_for_date(date_obj):
    result = {}
    for series, entries in macro_data_by_series.items():
        selected_value = None
        for entry_date, value in sorted(entries):
            if entry_date <= date_obj:
                selected_value = value
            else:
                break
        if selected_value is not None:
            result[series] = selected_value
    return result

def get_fundamentals(symbol, year):
    return fundamentals_by_symbol_year.get((symbol, year), {})

class FullDayAggregator(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.db_name = "aggregated-data"
        self.db_user = "admin"
        self.db_password = "admin123"
        self.db_host = os.getenv("POSTGRES_HOST", "localhost")
        self.db_port = os.getenv("POSTGRES_PORT", "5432")

        self.conn = None
        self.cursor = None
        self._connect_to_db()

    def _connect_to_db(self):
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
        table_name = "aggregated_data"

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
                y1 DOUBLE PRECISION, -- y5 rimosso
                PRIMARY KEY (ticker, timestamp)
            );
        """).format(table_name=sql.Identifier(table_name))
        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            print(f"[INFO] Table '{table_name}' checked/created successfully.")
        except Exception as e:
            print(f"[ERROR] Could not create table {table_name}: {e}", file=sys.stderr)
            self.conn.rollback()
            raise


    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("[INFO] PostgreSQL connection closed.")

    def process_element(self, value, ctx):
        try:
            message = json.loads(value)

            if "series" in message and "value" in message and "date" in message:
                try:
                    series = message["series"]
                    date = datetime.fromisoformat(message["date"]).date()
                    val = float(message["value"])
                    macro_data_by_series.setdefault(series, []).append((date, val))
                    macro_data_by_series[series].sort()
                    required_macro_series = ["gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"]
                    if all(s in macro_data_by_series for s in required_macro_series) and \
                       all(macro_data_by_series[s] for s in required_macro_series):
                        ready_flags["macro_ready"] = True
                except Exception as e:
                    print(f"[ERROR] Error processing macro data: {e} - Message: {message}", file=sys.stderr)
                return

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
                except Exception as e:
                    print(f"[ERROR] Error processing fundamental data: {e} - Message: {message}", file=sys.stderr)
                return

            if ready_flags["macro_ready"] and ready_flags["fundamentals_ready"] and pending_stock_batches:
                print(f"[INFO] Processing {len(pending_stock_batches)} buffered stock batches.", file=sys.stdout)
                for buffered_value in list(pending_stock_batches):
                    self._process_stock_message(json.loads(buffered_value))
                pending_stock_batches.clear()

            if not (ready_flags["macro_ready"] and ready_flags["fundamentals_ready"]):
                if "ticker" in message and "data" in message:
                    pending_stock_batches.append(value)
                return
            
            if "ticker" in message and "data" in message:
                self._process_stock_message(message)

        except Exception as e:
            print(f"[ERROR] Global error in process_element: {e} - Original Value: {value}", file=sys.stderr)

    def _process_stock_message(self, message):
        ticker = message.get("ticker")
        if ticker not in TOP_30_TICKERS:
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

            macro_values = get_macro_values_for_date(now.date())
            fundamentals = get_fundamentals(row["symbol"], now.year - 1)

            profit_margin = None
            if fundamentals.get("revenue") not in [None, 0]:
                profit_margin = fundamentals["netIncome"] / fundamentals["revenue"]

            debt_to_equity = None
            if fundamentals.get("balance_totalStockholdersEquity") not in [None, 0]:
                debt_to_equity = fundamentals["balance_totalDebt"] / fundamentals["balance_totalStockholdersEquity"]

            local_time = now.astimezone(ZoneInfo("America/New_York"))
            market_open_flag = int(dtime(9, 30) <= local_time.time() <= dtime(9, 34))
            market_close_flag = int(dtime(15, 56) <= local_time.time() <= dtime(16, 0))

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
                "sentiment_bluesky_mean_2hours": 0.0,
                "sentiment_bluesky_mean_1day": 0.0,
                "sentiment_news_mean_1day": 0.0,
                "sentiment_news_mean_3days": 0.0,
                "sentiment_general_bluesky_mean_2hours": 0.0,
                "sentiment_general_bluesky_mean_1day": 0.0,
                "minutes_since_open": (local_time - local_time.replace(hour=9, minute=30, second=0, microsecond=0)).total_seconds() // 60,
                "day_of_week": now.weekday(),
                "day_of_month": now.day,
                "week_of_year": now.isocalendar()[1],
                "month_of_year": now.month,
                "market_open_spike_flag": market_open_flag,
                "market_close_spike_flag": market_close_flag,
                "eps": fundamentals.get("eps"),
                "free_cash_flow": fundamentals.get("freeCashFlow"),
                "profit_margin": profit_margin,
                "debt_to_equity": debt_to_equity
            }

            result["gdp_real"] = macro_values.get("gdp_real", None)
            result["cpi"] = macro_values.get("cpi", None)
            result["ffr"] = macro_values.get("ffr", None)
            result["t10y"] = macro_values.get("t10y", None)
            result["t2y"] = macro_values.get("t2y", None)
            result["spread_10y_2y"] = macro_values.get("spread_10y_2y", None)
            result["unemployment"] = macro_values.get("unemployment", None)

            result["y1"] = row["close"]
            

            batch_results.append(result)

        if batch_results:
            self._insert_data_to_postgresql(batch_results)

    def _insert_data_to_postgresql(self, data):
        table_name = "aggregated_data"
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
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values_placeholder=values_placeholder
        )

        try:
            records_to_insert = []
            for record in data:
                record_copy = record.copy()
                record_copy["timestamp"] = datetime.fromisoformat(record_copy["timestamp"])
                
                row_values = [record_copy.get(col) for col in columns]
                records_to_insert.append(row_values)

            if records_to_insert:
                print(f"[INFO] First row of batch for insertion ({len(records_to_insert)} rows): {records_to_insert[0]}", file=sys.stdout)

            self.cursor.executemany(insert_query, records_to_insert)
            self.conn.commit()
            print(f"[INFO] Inserted {len(data)} records into {table_name}.", file=sys.stdout)
        except Exception as e:
            print(f"[ERROR] An error occurred during PostgreSQL insertion: {e}", file=sys.stderr)
            self.conn.rollback()


def extract_key(json_str):
    try:
        data = json.loads(json_str)
        return data.get("ticker") or data.get("symbol") or data.get("series") or "unknown"
    except json.JSONDecodeError:
        print(f"[ERROR] JSON decoding failed for: {json_str[:100]}...", file=sys.stderr)
        return "unknown"
    except Exception as e:
        print(f"[ERROR] Unexpected error in extract_key: {e} - String: {json_str[:100]}...", file=sys.stderr)
        return "unknown"

def main():
    db_name = "aggregated-data"
    db_user = "admin"
    db_password = "admin123"
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    table_name = "aggregated_data"

    try:
        conn_drop = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor_drop = conn_drop.cursor()
        drop_table_query = sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE;").format(
            table_name=sql.Identifier(table_name)
        )
        cursor_drop.execute(drop_table_query)
        conn_drop.commit()
        print(f"[INFO] Table '{table_name}' dropped successfully (if it existed) for a fresh start.", file=sys.stdout)
    except Exception as e:
        print(f"[ERROR] Could not drop table '{table_name}' at startup: {e}", file=sys.stderr)
    finally:
        if cursor_drop:
            cursor_drop.close()
        if conn_drop:
            conn_drop.close()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    consumer = FlinkKafkaConsumer(
        topics=["h_alpaca", "h_macrodata", "h_company"],
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": "flink_batch_group",
            "auto.offset.reset": "earliest"
        }
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    keyed = stream.key_by(extract_key, key_type=Types.STRING())
    processed = keyed.process(FullDayAggregator(), output_type=Types.ROW([]))

    env.execute("Historical Aggregation to PostgreSQL with Macro Data")

if __name__ == "__main__":
    main()












































































































































































































































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.datastream.state import MapStateDescriptor
# from datetime import datetime, timedelta, time as dtime
# from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
# import json
# import numpy as np
# import pandas as pd
# import os
# import sys
# import psycopg2
# from psycopg2 import sql

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# # Flink State Descriptors
# fundamentals_state_descriptor = MapStateDescriptor(
#     "fundamentals_data_state",
#     Types.INT(), # Key: year
#     Types.MAP(Types.STRING(), Types.DOUBLE()) # Value: dict of fundamental data, where values are doubles
# )

# pending_stock_batches_descriptor = MapStateDescriptor(
#     "pending_stock_batches_state",
#     Types.LONG(),    # Key: timestamp (or unique message ID)
#     Types.STRING()   # Value: original JSON message string
# )

# class FullDayAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         self.fundamentals_data_state = runtime_context.get_map_state(fundamentals_state_descriptor)
#         self.pending_stock_batches = runtime_context.get_map_state(pending_stock_batches_descriptor)

#         # These flags are local to each TaskManager and not fault-tolerant.
#         self._local_macro_data_by_series = {}
#         self._macro_ready_local = False
#         self._fundamentals_ready_local = False # This flag is local to the TaskManager

#         # Initialize PostgreSQL connection
#         self.pg_conn = psycopg2.connect(
#             host=os.getenv("POSTGRES_HOST", "postgre"),
#             port=os.getenv("POSTGRES_PORT", "5432"),
#             database=os.getenv("POSTGRES_DB", "aggregated-data"),
#             user=os.getenv("POSTGRES_USER", "admin"),
#             password=os.getenv("POSTGRES_PASSWORD", "admin123")
#         )
#         self.pg_conn.autocommit = False # Ensure transactions are used for safety
#         self.pg_cursor = self.pg_conn.cursor()

#         # --- PULIZIA DELLA TABELLA PRIMA DEL CARICAMENTO ---
#         try:
#             print("[INFO] Truncating 'aggregated_stock_data' table in PostgreSQL...")
#             truncate_query = "TRUNCATE TABLE aggregated_stock_data;"
#             self.pg_cursor.execute(truncate_query)
#             self.pg_conn.commit() # Commit the truncation
#             print("[INFO] Table 'aggregated_stock_data' truncated successfully.")
#         except Exception as e:
#             self.pg_conn.rollback() # Rollback in case of error
#             print(f"[ERROR] Failed to truncate table: {e}", file=sys.stderr)
#             # Consider if you want to re-raise the exception or let the job continue

#         # Create table if it doesn't exist
#         create_table_query = """
#         CREATE TABLE IF NOT EXISTS aggregated_stock_data (
#             id SERIAL PRIMARY KEY,
#             ticker VARCHAR(10) NOT NULL,
#             timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
#             price_mean_1min DOUBLE PRECISION,
#             price_mean_5min DOUBLE PRECISION,
#             price_std_5min DOUBLE PRECISION,
#             price_mean_30min DOUBLE PRECISION,
#             price_std_30min DOUBLE PRECISION,
#             size_tot_1min DOUBLE PRECISION,
#             size_tot_5min DOUBLE PRECISION,
#             size_tot_30min DOUBLE PRECISION,
#             sentiment_bluesky_mean_2hours DOUBLE PRECISION,
#             sentiment_bluesky_mean_1day DOUBLE PRECISION,
#             sentiment_news_mean_1day DOUBLE PRECISION,
#             sentiment_news_mean_3days DOUBLE PRECISION,
#             sentiment_general_bluesky_mean_2hours DOUBLE PRECISION,
#             sentiment_general_bluesky_mean_1day DOUBLE PRECISION,
#             minutes_since_open DOUBLE PRECISION,
#             day_of_week INTEGER,
#             day_of_month INTEGER,
#             week_of_year INTEGER,
#             month_of_year INTEGER,
#             market_open_spike_flag INTEGER,
#             market_close_spike_flag INTEGER,
#             y1 DOUBLE PRECISION,
#             y5 DOUBLE PRECISION
#         );
#         """
#         self.pg_cursor.execute(create_table_query)
#         self.pg_conn.commit()
#         print("Table 'aggregated_stock_data' verified/created in PostgreSQL.")

#     def close(self):
#         # Close PostgreSQL connection
#         if self.pg_cursor:
#             self.pg_cursor.close()
#         if self.pg_conn:
#             self.pg_conn.close()
#         print("PostgreSQL connection closed.")

#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)
#             current_key = ctx.get_current_key()

#             # Process real-time stock data
#             self._process_stock_data(value, ctx)

#         except json.JSONDecodeError as e:
#             print(f"[ERROR] JSON decoding failed: {e} for value: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"[ERROR] General processing error: {e}", file=sys.stderr)

#     def _process_stock_data(self, value_str, ctx):
#         """Helper method to process stock data messages."""
#         message = json.loads(value_str)

#         ticker = message.get("ticker")
#         if ticker not in TOP_30_TICKERS:
#             return

#         parsed = []
#         # Define New York timezone once for this method call
#         try:
#             ny_timezone = ZoneInfo("America/New_York")
#         except ZoneInfoNotFoundError:
#             print("[ERROR] 'America/New_York' timezone not found. Ensure tzdata is installed or use a fallback.", file=sys.stderr)
#             ny_timezone = ZoneInfo("UTC") # Fallback, but this will lead to inconsistent results

#         for entry in message.get("data", []):
#             try:
#                 # Convert original timestamp (UTC) to a timezone-aware datetime object (still UTC)
#                 timestamp_utc = datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
#                 # Convert it immediately to New York time for all subsequent calculations
#                 timestamp_ny = timestamp_utc.astimezone(ny_timezone)

#                 parsed.append({
#                     "symbol": ticker,
#                     "timestamp_utc": timestamp_utc, # Keep original UTC for DB storage
#                     "timestamp_ny": timestamp_ny,   # New York time for calculations
#                     "open": float(entry["open"]),
#                     "close": float(entry["close"]),
#                     "volume": float(entry["volume"]),
#                 })
#             except (ValueError, KeyError) as e:
#                 print(f"[WARN] Failed to parse stock entry: {e} - {entry}", file=sys.stderr)
#                 continue

#         parsed.sort(key=lambda x: x["timestamp_utc"]) # Sort by UTC to preserve original order
#         batch_results = []

#         for i in range(len(parsed)):
#             row = parsed[i]
#             # Use timestamp_ny for all time-based feature extractions
#             current_timestamp_ny = row["timestamp_ny"]
#             original_timestamp_utc = row["timestamp_utc"] # This goes to the database

#             def window_vals(field, minutes):
#                 # Windowing should still be based on the consistent UTC time for the raw data
#                 ts_limit_utc = original_timestamp_utc - timedelta(minutes=minutes)
#                 return [p[field] for p in parsed if ts_limit_utc <= p["timestamp_utc"] <= original_timestamp_utc]

#             def mean(values): return float(np.mean(values)) if values else 0.0 # Default to 0.0 instead of None
#             def std(values): return float(np.std(values)) if values else 0.0 # Default to 0.0 instead of None
#             def total(values): return float(np.sum(values)) if values else 0.0


#             # Calculate market open/close flags based on New York time
#             market_open_flag = int(dtime(9, 30, tzinfo=ny_timezone) <= current_timestamp_ny.time() <= dtime(9, 34, tzinfo=ny_timezone))
#             market_close_flag = int(dtime(15, 56, tzinfo=ny_timezone) <= current_timestamp_ny.time() <= dtime(16, 0, tzinfo=ny_timezone))

#             # Calculate minutes_since_open based on New York time
#             market_open_ny_today = current_timestamp_ny.replace(hour=9, minute=30, second=0, microsecond=0)
            
#             # Ensure market_open_ny_today is also timezone-aware
#             if market_open_ny_today.tzinfo is None:
#                 market_open_ny_today = market_open_ny_today.replace(tzinfo=ny_timezone)

#             if current_timestamp_ny >= market_open_ny_today:
#                 minutes_since_open = (current_timestamp_ny - market_open_ny_today).total_seconds() // 60
#             else:
#                 minutes_since_open = 0.0 


#             result = {
#                 "ticker": row["symbol"],
#                 "timestamp": original_timestamp_utc.isoformat(), # Store original UTC in DB
#                 "price_mean_1min": mean(window_vals("open", 1)),
#                 "price_mean_5min": mean(window_vals("open", 5)),
#                 "price_std_5min": std(window_vals("open", 5)),
#                 "price_mean_30min": mean(window_vals("open", 30)),
#                 "price_std_30min": std(window_vals("open", 30)),
#                 "size_tot_1min": total(window_vals("volume", 1)),
#                 "size_tot_5min": total(window_vals("volume", 5)),
#                 "size_tot_30min": total(window_vals("volume", 30)),
#                 "sentiment_bluesky_mean_2hours": 0.0,
#                 "sentiment_bluesky_mean_1day": 0.0,
#                 "sentiment_news_mean_1day": 0.0,
#                 "sentiment_news_mean_3days": 0.0,
#                 "sentiment_general_bluesky_mean_2hours": 0.0,
#                 "sentiment_general_bluesky_mean_1day": 0.0,
#                 "minutes_since_open": minutes_since_open,
#                 "day_of_week": current_timestamp_ny.weekday(), # Based on NY time
#                 "day_of_month": current_timestamp_ny.day,     # Based on NY time
#                 "week_of_year": current_timestamp_ny.isocalendar()[1], # Based on NY time
#                 "month_of_year": current_timestamp_ny.month,   # Based on NY time
#                 "market_open_spike_flag": market_open_flag,
#                 "market_close_spike_flag": market_close_flag,
#             }


#             result["y1"] = row["close"]
#             if i <= len(parsed) - 5:
#                 result["y5"] = parsed[i + 4]["close"]
#             else:
#                 result["y5"] = 0.0 # Default to 0.0 if y5 cannot be calculated

#             batch_results.append(result)

#         # Write to PostgreSQL
#         if batch_results:
#             self.pg_cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'aggregated_stock_data' ORDER BY ordinal_position;")
#             db_columns = [row[0] for row in self.pg_cursor.fetchall()]
            
#             insert_columns = [col for col in db_columns if col != 'id']
            
#             placeholders = ', '.join(['%s'] * len(insert_columns))
#             column_names_sql = ', '.join([sql.Identifier(col).as_string(self.pg_conn) for col in insert_columns])
            
#             insert_query = sql.SQL(
#                 "INSERT INTO aggregated_stock_data ({}) VALUES ({})"
#             ).format(sql.SQL(column_names_sql), sql.SQL(placeholders))

#             records_to_insert = []
#             for record in batch_results:
#                 # Ensure the timestamp going into DB is the original UTC one, but convert it to datetime object
#                 record["timestamp"] = datetime.fromisoformat(record["timestamp"]) 
                
#                 row_values = []
#                 for col in insert_columns:
#                     # Ensure all values are not None. If a key is missing or None, default to 0.0 or an appropriate type default.
#                     value_to_insert = record.get(col)
#                     if value_to_insert is None:
#                         # Apply a default based on expected type if None is encountered
#                         if col in ["ticker", "timestamp"]: # These should always be present
#                             row_values.append(value_to_insert) # Let psycopg2 handle if still None, but ideally they shouldn't be
#                         elif col in ["day_of_week", "day_of_month", "week_of_year", "month_of_year", "market_open_spike_flag", "market_close_spike_flag"]:
#                             row_values.append(0) # Default for INTEGER
#                         else:
#                             row_values.append(0.0) # Default for DOUBLE PRECISION
#                     else:
#                         row_values.append(value_to_insert) 
#                 records_to_insert.append(tuple(row_values))

#             try:
#                 self.pg_cursor.executemany(insert_query, records_to_insert)
#                 self.pg_conn.commit()
#                 if records_to_insert:
#                     first_record_timestamp = records_to_insert[0][insert_columns.index("timestamp")]
#                     print(f"[{first_record_timestamp}] Inserted {len(records_to_insert)} rows into PostgreSQL for {ticker}. First row: {records_to_insert[0]}")
#             except Exception as e:
#                 self.pg_conn.rollback()
#                 print(f"[ERROR] Failed to insert into PostgreSQL: {e}", file=sys.stderr)

# def extract_key(json_str):
#     try:
#         data = json.loads(json_str)
#         if "series" in data:
#             return "GLOBAL_MACRO_KEY" # All macro data goes to a fixed key
#         elif "symbol" in data:
#             return data["symbol"]
#         elif "ticker" in data:
#             return data["ticker"]
#         return "unknown"
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     # Set parallelism to 1 for local development.
#     # If deploying to a Flink cluster, this line can be removed or set to a higher value
#     # to leverage cluster resources.
#     env.set_parallelism(1) 

#     consumer = FlinkKafkaConsumer(
#         topics=["h_alpaca"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
#             "group.id": "flink_postgres_aggregator_v2",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
    
#     keyed = stream.key_by(extract_key, key_type=Types.STRING())
#     processed = keyed.process(FullDayAggregator(), output_type=Types.STRING())
 
#     env.execute("Historical Aggregation to PostgreSQL (Distributed)")

# if __name__ == "__main__":
#     main()



















































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.datastream.state import MapStateDescriptor
# from datetime import datetime, timedelta, time as dtime
# from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
# import json
# import numpy as np
# import pandas as pd
# import os
# import sys
# import psycopg2
# from psycopg2 import sql

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# # Flink State Descriptors
# fundamentals_state_descriptor = MapStateDescriptor(
#     "fundamentals_data_state",
#     Types.INT(), # Key: year
#     Types.MAP(Types.STRING(), Types.DOUBLE()) # Value: dict of fundamental data, where values are doubles
# )

# pending_stock_batches_descriptor = MapStateDescriptor(
#     "pending_stock_batches_state",
#     Types.LONG(),    # Key: timestamp (or unique message ID)
#     Types.STRING()   # Value: original JSON message string
# )

# # New Flink State Descriptor for Macro Data
# macro_data_state_descriptor = MapStateDescriptor(
#     "macro_data_state",
#     Types.STRING(), # Key: macro series alias (e.g., "gdp_real", "cpi")
#     Types.TUPLE([Types.STRING(), Types.DOUBLE()]) # Value: tuple of (date string, value)
# )

# class FullDayAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         self.fundamentals_data_state = runtime_context.get_map_state(fundamentals_state_descriptor)
#         self.pending_stock_batches = runtime_context.get_map_state(pending_stock_batches_descriptor)
#         self.macro_data_state = runtime_context.get_map_state(macro_data_state_descriptor) # Initialize macro state

#         # These flags are local to each TaskManager and not fault-tolerant.
#         self._local_macro_data_by_series = {}
#         self._macro_ready_local = False
#         self._fundamentals_ready_local = False # This flag is local to the TaskManager

#         # Initialize PostgreSQL connection
#         self.pg_conn = psycopg2.connect(
#             host=os.getenv("POSTGRES_HOST", "postgre"),
#             port=os.getenv("POSTGRES_PORT", "5432"),
#             database=os.getenv("POSTGRES_DB", "aggregated-data"),
#             user=os.getenv("POSTGRES_USER", "admin"),
#             password=os.getenv("POSTGRES_PASSWORD", "admin123")
#         )
#         self.pg_conn.autocommit = False # Ensure transactions are used for safety
#         self.pg_cursor = self.pg_conn.cursor()

#         # --- PULIZIA DELLA TABELLA PRIMA DEL CARICAMENTO ---
#         try:
#             print("[INFO] Truncating 'aggregated_stock_data' table in PostgreSQL...")
#             truncate_query = "TRUNCATE TABLE aggregated_stock_data;"
#             self.pg_cursor.execute(truncate_query)
#             self.pg_conn.commit() # Commit the truncation
#             print("[INFO] Table 'aggregated_stock_data' truncated successfully.")
#         except Exception as e:
#             self.pg_conn.rollback() # Rollback in case of error
#             print(f"[ERROR] Failed to truncate table: {e}", file=sys.stderr)
#             # Consider if you want to re-raise the exception or let the job continue

#         # Create table if it doesn't exist
#         create_table_query = """
#         CREATE TABLE IF NOT EXISTS aggregated_stock_data (
#             id SERIAL PRIMARY KEY,
#             ticker VARCHAR(10) NOT NULL,
#             timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
#             price_mean_1min DOUBLE PRECISION,
#             price_mean_5min DOUBLE PRECISION,
#             price_std_5min DOUBLE PRECISION,
#             price_mean_30min DOUBLE PRECISION,
#             price_std_30min DOUBLE PRECISION,
#             size_tot_1min DOUBLE PRECISION,
#             size_tot_5min DOUBLE PRECISION,
#             size_tot_30min DOUBLE PRECISION,
#             sentiment_bluesky_mean_2hours DOUBLE PRECISION,
#             sentiment_bluesky_mean_1day DOUBLE PRECISION,
#             sentiment_news_mean_1day DOUBLE PRECISION,
#             sentiment_news_mean_3days DOUBLE PRECISION,
#             sentiment_general_bluesky_mean_2hours DOUBLE PRECISION,
#             sentiment_general_bluesky_mean_1day DOUBLE PRECISION,
#             minutes_since_open DOUBLE PRECISION,
#             day_of_week INTEGER,
#             day_of_month INTEGER,
#             week_of_year INTEGER,
#             month_of_year INTEGER,
#             market_open_spike_flag INTEGER,
#             market_close_spike_flag INTEGER,
#             gdp_real DOUBLE PRECISION, 
#             cpi DOUBLE PRECISION, 
#             ffr DOUBLE PRECISION, 
#             t10y DOUBLE PRECISION, 
#             t2y DOUBLE PRECISION, 
#             spread_10y_2y DOUBLE PRECISION, 
#             unemployment DOUBLE PRECISION, 
#             y1 DOUBLE PRECISION,
#             y5 DOUBLE PRECISION
#         );
#         """
#         self.pg_cursor.execute(create_table_query)
#         self.pg_conn.commit()
#         print("Table 'aggregated_stock_data' verified/created in PostgreSQL.")

#     def close(self):
#         # Close PostgreSQL connection
#         if self.pg_cursor:
#             self.pg_cursor.close()
#         if self.pg_conn:
#             self.pg_conn.close()
#         print("PostgreSQL connection closed.")

#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)
#             current_key = ctx.get_current_key()

#             if "series" in message:
#                 # This is a macro data message
#                 self._process_macro_data(message, ctx)
#             else:
#                 # This is a stock data message
#                 self._process_stock_data(value, ctx)

#         except json.JSONDecodeError as e:
#             print(f"[ERROR] JSON decoding failed: {e} for value: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"[ERROR] General processing error: {e}", file=sys.stderr)

#     def _process_macro_data(self, message, ctx):
#         """Helper method to process macro data messages."""
#         series_alias = message.get("series")
#         date_str = message.get("date")
#         value = message.get("value")

#         print(f"[DEBUG_MACRO] Received macro message: {message}") # <--- NUOVO PRINT DI DEBUG

#         if series_alias and date_str and value is not None:
#             # Get current value from state to compare
#             current_state_tuple = self.macro_data_state.get(series_alias)
#             current_state_date_str = current_state_tuple[0] if current_state_tuple else "1900-01-01"

#             # Convert to datetime objects for comparison
#             new_date = datetime.strptime(date_str, "%Y-%m-%d")
#             current_state_date = datetime.strptime(current_state_date_str, "%Y-%m-%d")

#             print(f"[DEBUG_MACRO] Processing {series_alias}. New date: {new_date}, Current state date: {current_state_date}") # <--- NUOVO PRINT DI DEBUG

#             # Only update if the new data is more recent
#             if new_date > current_state_date:
#                 self.macro_data_state.put(series_alias, (date_str, value))
#                 print(f"[INFO] Updated macro data for {series_alias}: {value} (Date: {date_str})") # <--- Questo era già presente ma assicurati che venga mostrato
#             else:
#                 print(f"[INFO] Received older/same macro data for {series_alias}. Current: {current_state_date_str}, New: {date_str}") # <--- NUOVO PRINT PER DATI VECCHI
#         else:
#             print(f"[WARN] Incomplete macro data message: {message}", file=sys.stderr)

#     def _process_stock_data(self, value_str, ctx):
#         """Helper method to process stock data messages."""
#         message = json.loads(value_str)

#         ticker = message.get("ticker")
#         if ticker not in TOP_30_TICKERS:
#             return

#         parsed = []
#         # Define New York timezone once for this method call
#         try:
#             ny_timezone = ZoneInfo("America/New_York")
#         except ZoneInfoNotFoundError:
#             print("[ERROR] 'America/New_York' timezone not found. Ensure tzdata is installed or use a fallback.", file=sys.stderr)
#             ny_timezone = ZoneInfo("UTC") # Fallback, but this will lead to inconsistent results

#         for entry in message.get("data", []):
#             try:
#                 # Convert original timestamp (UTC) to a timezone-aware datetime object (still UTC)
#                 timestamp_utc = datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
#                 # Convert it immediately to New York time for all subsequent calculations
#                 timestamp_ny = timestamp_utc.astimezone(ny_timezone)

#                 parsed.append({
#                     "symbol": ticker,
#                     "timestamp_utc": timestamp_utc, # Keep original UTC for DB storage
#                     "timestamp_ny": timestamp_ny,  # New York time for calculations
#                     "open": float(entry["open"]),
#                     "close": float(entry["close"]),
#                     "volume": float(entry["volume"]),
#                 })
#             except (ValueError, KeyError) as e:
#                 print(f"[WARN] Failed to parse stock entry: {e} - {entry}", file=sys.stderr)
#                 continue

#         parsed.sort(key=lambda x: x["timestamp_utc"]) # Sort by UTC to preserve original order
#         batch_results = []

#         # Fetch current macro data from state once per batch
#         current_macro_data = {}
#         for series_alias_entry in ["gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"]:
#             macro_tuple = self.macro_data_state.get(series_alias_entry)
#             if macro_tuple:
#                 current_macro_data[series_alias_entry] = macro_tuple[1] # Store just the value
#             else:
#                 current_macro_data[series_alias_entry] = 0.0 # Default value if no data found

#         for i in range(len(parsed)):
#             row = parsed[i]
#             # Use timestamp_ny for all time-based feature extractions
#             current_timestamp_ny = row["timestamp_ny"]
#             original_timestamp_utc = row["timestamp_utc"] # This goes to the database

#             def window_vals(field, minutes):
#                 # Windowing should still be based on the consistent UTC time for the raw data
#                 ts_limit_utc = original_timestamp_utc - timedelta(minutes=minutes)
#                 return [p[field] for p in parsed if ts_limit_utc <= p["timestamp_utc"] <= original_timestamp_utc]

#             def mean(values): return float(np.mean(values)) if values else 0.0 # Default to 0.0 instead of None
#             def std(values): return float(np.std(values)) if values else 0.0 # Default to 0.0 instead of None
#             def total(values): return float(np.sum(values)) if values else 0.0


#             # Calculate market open/close flags based on New York time
#             market_open_flag = int(dtime(9, 30, tzinfo=ny_timezone) <= current_timestamp_ny.time() <= dtime(9, 34, tzinfo=ny_timezone))
#             market_close_flag = int(dtime(15, 56, tzinfo=ny_timezone) <= current_timestamp_ny.time() <= dtime(16, 0, tzinfo=ny_timezone))

#             # Calculate minutes_since_open based on New York time
#             market_open_ny_today = current_timestamp_ny.replace(hour=9, minute=30, second=0, microsecond=0)
            
#             # Ensure market_open_ny_today is also timezone-aware
#             if market_open_ny_today.tzinfo is None:
#                 market_open_ny_today = market_open_ny_today.replace(tzinfo=ny_timezone)

#             if current_timestamp_ny >= market_open_ny_today:
#                 minutes_since_open = (current_timestamp_ny - market_open_ny_today).total_seconds() // 60
#             else:
#                 minutes_since_open = 0.0 


#             result = {
#                 "ticker": row["symbol"],
#                 "timestamp": original_timestamp_utc.isoformat(), # Store original UTC in DB
#                 "price_mean_1min": mean(window_vals("open", 1)),
#                 "price_mean_5min": mean(window_vals("open", 5)),
#                 "price_std_5min": std(window_vals("open", 5)),
#                 "price_mean_30min": mean(window_vals("open", 30)),
#                 "price_std_30min": std(window_vals("open", 30)),
#                 "size_tot_1min": total(window_vals("volume", 1)),
#                 "size_tot_5min": total(window_vals("volume", 5)),
#                 "size_tot_30min": total(window_vals("volume", 30)),
#                 "sentiment_bluesky_mean_2hours": 0.0,
#                 "sentiment_bluesky_mean_1day": 0.0,
#                 "sentiment_news_mean_1day": 0.0,
#                 "sentiment_news_mean_3days": 0.0,
#                 "sentiment_general_bluesky_mean_2hours": 0.0,
#                 "sentiment_general_bluesky_mean_1day": 0.0,
#                 "minutes_since_open": minutes_since_open,
#                 "day_of_week": current_timestamp_ny.weekday(), # Based on NY time
#                 "day_of_month": current_timestamp_ny.day,     # Based on NY time
#                 "week_of_year": current_timestamp_ny.isocalendar()[1], # Based on NY time
#                 "month_of_year": current_timestamp_ny.month,  # Based on NY time
#                 "market_open_spike_flag": market_open_flag,
#                 "market_close_spike_flag": market_close_flag,
#             }

#             # Add macro data to the result
#             result.update(current_macro_data)

#             result["y1"] = row["close"]
#             if i <= len(parsed) - 5:
#                 result["y5"] = parsed[i + 4]["close"]
#             else:
#                 result["y5"] = None # Default to None if y5 cannot be calculated

#             batch_results.append(result)

#         # Write to PostgreSQL
#         if batch_results:
#             self.pg_cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'aggregated_stock_data' ORDER BY ordinal_position;")
#             db_columns = [row[0] for row in self.pg_cursor.fetchall()]
            
#             insert_columns = [col for col in db_columns if col != 'id']
            
#             placeholders = ', '.join(['%s'] * len(insert_columns))
#             column_names_sql = ', '.join([sql.Identifier(col).as_string(self.pg_conn) for col in insert_columns])
            
#             insert_query = sql.SQL(
#                 "INSERT INTO aggregated_stock_data ({}) VALUES ({})"
#             ).format(sql.SQL(column_names_sql), sql.SQL(placeholders))

#             records_to_insert = []
#             for record in batch_results:
#                 # Ensure the timestamp going into DB is the original UTC one, but convert it to datetime object
#                 record["timestamp"] = datetime.fromisoformat(record["timestamp"]) 
                
#                 row_values = []
#                 for col in insert_columns:
#                     # Ensure all values are not None. If a key is missing or None, default to 0.0 or an appropriate type default.
#                     value_to_insert = record.get(col)
#                     if value_to_insert is None:
#                         # Apply a default based on expected type if None is encountered
#                         if col in ["ticker", "timestamp"]: # These should always be present
#                             row_values.append(value_to_insert) # Let psycopg2 handle if still None, but ideally they shouldn't be
#                         elif col in ["day_of_week", "day_of_month", "week_of_year", "month_of_year", "market_open_spike_flag", "market_close_spike_flag"]:
#                             row_values.append(0) # Default for INTEGER
#                         else:
#                             row_values.append(0.0) # Default for DOUBLE PRECISION
#                     else:
#                         row_values.append(value_to_insert) 
#                 records_to_insert.append(tuple(row_values))

#             try:
#                 self.pg_cursor.executemany(insert_query, records_to_insert)
#                 self.pg_conn.commit()
#                 if records_to_insert:
#                     first_record_timestamp = records_to_insert[0][insert_columns.index("timestamp")]
#                     print(f"[{first_record_timestamp}] Inserted {len(records_to_insert)} rows into PostgreSQL for {ticker}. First row: {records_to_insert[0]}")
#                     print(f"[{first_record_timestamp}] Inserted {len(records_to_insert)} rows into PostgreSQL for {ticker}. First row: {records_to_insert[0]}. Macro data used: {current_macro_data}")
#             except Exception as e:
#                 self.pg_conn.rollback()
#                 print(f"[ERROR] Failed to insert into PostgreSQL: {e}", file=sys.stderr)

# def extract_key(json_str):
#     try:
#         data = json.loads(json_str)
#         if "series" in data:
#             # Macro data: key by the series alias itself
#             return data["series"] 
#         elif "symbol" in data:
#             return data["symbol"]
#         elif "ticker" in data:
#             return data["ticker"]
#         return "unknown"
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     # Set parallelism to 1 for local development.
#     # If deploying to a Flink cluster, this line can be removed or set to a higher value
#     # to leverage cluster resources.
#     env.set_parallelism(1) 

#     # Consume from both Kafka topics
#     stock_consumer = FlinkKafkaConsumer(
#         topics=["h_alpaca"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
#             "group.id": "flink_postgres_aggregator_stock_v2",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     macro_consumer = FlinkKafkaConsumer(
#         topics=["h_macrodata"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
#             "group.id": "flink_postgres_aggregator_macro_v2",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     stock_stream = env.add_source(stock_consumer, type_info=Types.STRING())
#     macro_stream = env.add_source(macro_consumer, type_info=Types.STRING())

#     # Merge the two streams
#     # Union requires both streams to have the same type, which they do (Strings)
#     unified_stream = stock_stream.union(macro_stream)
    
#     # Key by ticker for stock data and series_alias for macro data
#     keyed = unified_stream.key_by(extract_key, key_type=Types.STRING())
    
#     processed = keyed.process(FullDayAggregator(), output_type=Types.STRING())
    
#     env.execute("Historical Aggregation to PostgreSQL (Distributed)")

# if __name__ == "__main__":
#     main()
















































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.datastream.state import MapStateDescriptor
# from datetime import datetime, timedelta, time as dtime
# from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
# import json
# import numpy as np
# import pandas as pd
# import os
# import sys
# import psycopg2
# from psycopg2 import sql

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# fundamentals_state_descriptor = MapStateDescriptor(
#     "fundamentals_data_state",
#     Types.INT(),
#     Types.MAP(Types.STRING(), Types.DOUBLE())
# )

# pending_stock_batches_descriptor = MapStateDescriptor(
#     "pending_stock_batches_state",
#     Types.LONG(),
#     Types.STRING()
# )

# macro_data_state_descriptor = MapStateDescriptor(
#     "macro_data_state",
#     Types.STRING(),
#     Types.MAP(Types.STRING(), Types.DOUBLE())
# )


# class FullDayAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         self.fundamentals_data_state = runtime_context.get_map_state(fundamentals_state_descriptor)
#         self.pending_stock_batches = runtime_context.get_map_state(pending_stock_batches_descriptor)
#         self.macro_data_state = runtime_context.get_map_state(macro_data_state_descriptor)

#         self.pg_conn = psycopg2.connect(
#             host=os.getenv("POSTGRES_HOST", "postgre"),
#             port=os.getenv("POSTGRES_PORT", "5432"),
#             database=os.getenv("POSTGRES_DB", "aggregated-data"),
#             user=os.getenv("POSTGRES_USER", "admin"),
#             password=os.getenv("POSTGRES_PASSWORD", "admin123")
#         )
#         self.pg_conn.autocommit = False
#         self.pg_cursor = self.pg_conn.cursor()

#         try:
#             print("[INFO] Dropping and recreating 'aggregated_stock_data' table in PostgreSQL...", flush=True)
#             self.pg_cursor.execute("DROP TABLE IF EXISTS aggregated_stock_data;")
#             self.pg_conn.commit()
#         except Exception as e:
#             self.pg_conn.rollback()
#             print(f"[ERROR] Failed to drop table: {e}", file=sys.stderr, flush=True)
        
#         self.pg_cursor.execute("""
#             CREATE TABLE IF NOT EXISTS aggregated_stock_data (
#                 id SERIAL PRIMARY KEY,
#                 ticker VARCHAR(10) NOT NULL,
#                 timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
#                 price_mean_1min DOUBLE PRECISION,
#                 price_mean_5min DOUBLE PRECISION,
#                 price_std_5min DOUBLE PRECISION,
#                 price_mean_30min DOUBLE PRECISION,
#                 price_std_30min DOUBLE PRECISION,
#                 size_tot_1min DOUBLE PRECISION,
#                 size_tot_5min DOUBLE PRECISION,
#                 size_tot_30min DOUBLE PRECISION,
#                 sentiment_bluesky_mean_2hours DOUBLE PRECISION,
#                 sentiment_bluesky_mean_1day DOUBLE PRECISION,
#                 sentiment_news_mean_1day DOUBLE PRECISION,
#                 sentiment_news_mean_3days DOUBLE PRECISION,
#                 sentiment_general_bluesky_mean_2hours DOUBLE PRECISION,
#                 sentiment_general_bluesky_mean_1day DOUBLE PRECISION,
#                 minutes_since_open DOUBLE PRECISION,
#                 day_of_week INTEGER,
#                 day_of_month INTEGER,
#                 week_of_year INTEGER,
#                 month_of_year INTEGER,
#                 market_open_spike_flag INTEGER,
#                 market_close_spike_flag INTEGER,
#                 gdp_real DOUBLE PRECISION,
#                 cpi DOUBLE PRECISION,
#                 ffr DOUBLE PRECISION,
#                 t10y DOUBLE PRECISION,
#                 t2y DOUBLE PRECISION,
#                 spread_10y_2y DOUBLE PRECISION,
#                 unemployment DOUBLE PRECISION,
#                 y1 DOUBLE PRECISION,
#                 y5 DOUBLE PRECISION
#             );
#         """)

#         self.pg_conn.commit()

#     def close(self):
#         if self.pg_cursor:
#             self.pg_cursor.close()
#         if self.pg_conn:
#             self.pg_conn.close()

#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)
#             if "series" in message:
#                 self._process_macro_data(message)
#             else:
#                 self._process_stock_data(value)
#         except Exception as e:
#             print(f"[ERROR] Processing error: {e}", file=sys.stderr, flush=True)

#     def _process_macro_data(self, message):
#         series_alias = message.get("series")
#         date_str = message.get("date")
#         value = message.get("value")

#         if series_alias and date_str and value is not None:
#             current_map = dict(self.macro_data_state.get(series_alias) or {})
#             current_map[date_str] = value
#             self.macro_data_state.put(series_alias, current_map)
#             print(f"[DEBUG] Updated macro series {series_alias} with {date_str} = {value}")


#     def _process_stock_data(self, value_str):
#         message = json.loads(value_str)
#         ticker = message.get("ticker")
#         if ticker not in TOP_30_TICKERS:
#             return

#         try:
#             ny_tz = ZoneInfo("America/New_York")
#         except:
#             ny_tz = ZoneInfo("UTC")

#         parsed = []
#         for entry in message.get("data", []):
#             try:
#                 timestamp_utc = datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
#                 timestamp_ny = timestamp_utc.astimezone(ny_tz)
#                 parsed.append({
#                     "symbol": ticker,
#                     "timestamp_utc": timestamp_utc,
#                     "timestamp_ny": timestamp_ny,
#                     "open": float(entry["open"]),
#                     "close": float(entry["close"]),
#                     "volume": float(entry["volume"]),
#                 })
#             except Exception:
#                 continue

#         parsed.sort(key=lambda x: x["timestamp_utc"])

#         def get_latest_macro_value(series, ts):
#             series_map = self.macro_data_state.get(series)
#             if not series_map:
#                 print(f"[DEBUG] No data for macro series: {series}")
#                 return 0.0

#             ts_date = ts.date()
#             available = []
#             for d in series_map:
#                 try:
#                     d_parsed = datetime.strptime(d, "%Y-%m-%d").date()
#                     if d_parsed <= ts_date:
#                         available.append(d_parsed)
#                 except Exception as e:
#                     print(f"[DEBUG] Failed parsing date {d}: {e}")

#             if not available:
#                 print(f"[DEBUG] No matching macro date for {series} at {ts_date}")
#                 return 0.0

#             latest = max(available).strftime("%Y-%m-%d")
#             matched_value = series_map[latest]
#             print(f"[DEBUG] Macro match for {series} at {ts_date} → {latest} = {matched_value}")
#             return matched_value


#         printed_first = False

#         for row in parsed:
#             ts_ny = row["timestamp_ny"]
#             ts_utc = row["timestamp_utc"]

#             market_open_flag = int(dtime(9, 30) <= ts_ny.time() <= dtime(9, 34))
#             market_close_flag = int(dtime(15, 56) <= ts_ny.time() <= dtime(16, 0))
#             market_open = ts_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#             minutes_since_open = max(0.0, (ts_ny - market_open).total_seconds() / 60)

#             result = {
#                 "ticker": row["symbol"],
#                 "timestamp": ts_utc,
#                 "price_mean_1min": row["open"],
#                 "price_mean_5min": row["open"],
#                 "price_std_5min": 0.0,
#                 "price_mean_30min": row["open"],
#                 "price_std_30min": 0.0,
#                 "size_tot_1min": row["volume"],
#                 "size_tot_5min": row["volume"],
#                 "size_tot_30min": row["volume"],
#                 "sentiment_bluesky_mean_2hours": 0.0,
#                 "sentiment_bluesky_mean_1day": 0.0,
#                 "sentiment_news_mean_1day": 0.0,
#                 "sentiment_news_mean_3days": 0.0,
#                 "sentiment_general_bluesky_mean_2hours": 0.0,
#                 "sentiment_general_bluesky_mean_1day": 0.0,
#                 "minutes_since_open": minutes_since_open,
#                 "day_of_week": ts_ny.weekday(),
#                 "day_of_month": ts_ny.day,
#                 "week_of_year": ts_ny.isocalendar()[1],
#                 "month_of_year": ts_ny.month,
#                 "market_open_spike_flag": market_open_flag,
#                 "market_close_spike_flag": market_close_flag,
#                 "gdp_real": get_latest_macro_value("gdp_real", ts_utc),
#                 "cpi": get_latest_macro_value("cpi", ts_utc),
#                 "ffr": get_latest_macro_value("ffr", ts_utc),
#                 "t10y": get_latest_macro_value("t10y", ts_utc),
#                 "t2y": get_latest_macro_value("t2y", ts_utc),
#                 "spread_10y_2y": get_latest_macro_value("spread_10y_2y", ts_utc),
#                 "unemployment": get_latest_macro_value("unemployment", ts_utc),
#                 "y1": row["close"],
#                 "y5": row["close"]
#             }

#             if not printed_first:
#                 print(f"[DEBUG] Primo valore aggregato per {ticker}: {result}", flush=True)
#                 printed_first = True

#             columns = list(result.keys())
#             values = [result[col] for col in columns]

#             insert_query = sql.SQL("INSERT INTO aggregated_stock_data ({}) VALUES ({})").format(
#                 sql.SQL(', ').join(map(sql.Identifier, columns)),
#                 sql.SQL(', ').join(sql.Placeholder() * len(columns))
#             )

#             try:
#                 self.pg_cursor.execute(insert_query, values)
#                 self.pg_conn.commit()
#             except Exception as e:
#                 self.pg_conn.rollback()
#                 print(f"[ERROR] Failed to insert: {e}", file=sys.stderr, flush=True)


# def extract_key(json_str):
#     try:
#         data = json.loads(json_str)
#         if "series" in data:
#             return data["series"]
#         elif "ticker" in data:
#             return data["ticker"]
#         return "unknown"
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     stock_consumer = FlinkKafkaConsumer(
#         topics=["h_alpaca"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
#             "group.id": "flink_postgres_aggregator_stock",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     macro_consumer = FlinkKafkaConsumer(
#         topics=["h_macrodata"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
#             "group.id": "flink_postgres_aggregator_macro",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     stream = env.add_source(stock_consumer).union(env.add_source(macro_consumer)).key_by(extract_key, key_type=Types.STRING())
#     stream.process(FullDayAggregator())
#     env.execute("Flink Stock + Macro Aggregation")

# if __name__ == "__main__":
#     main()



















































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment, DataTypes
# from pyflink.table.expressions import col, lit
# from pyflink.table.udf import udf
# import os
# from datetime import datetime
# from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
# import sys



# # Define New York timezone globally for UDFs if needed, though SQL functions are preferred
# try:
#     NY_TIMEZONE = ZoneInfo("America/New_York")
# except ZoneInfoNotFoundError:
#     print("[ERROR] 'America/New_York' timezone not found. Falling back to UTC.", file=sys.stderr)
#     NY_TIMEZONE = ZoneInfo("UTC")

# # --- User-Defined Functions (UDFs) for PyFlink Table API ---
# # These UDFs replicate some of the logic from your original KeyedProcessFunction
# # You'll need to install numpy if you plan to use it within UDFs (pip install numpy)
# # For simple mean/std on windowed data, SQL window functions might be more efficient
# # if you move more logic to SQL.

# # Scalar UDF to calculate minutes since market open
# @udf(result_type=DataTypes.DOUBLE())
# def minutes_since_market_open_udf(timestamp_utc: datetime) -> float:
#     """Calculates minutes since market open (9:30 AM NY time) for a given timestamp."""
#     # Convert UTC timestamp to New York timezone for market hours logic
#     timestamp_ny = timestamp_utc.astimezone(NY_TIMEZONE)
#     market_open_ny_today = timestamp_ny.replace(hour=9, minute=30, second=0, microsecond=0)

#     if timestamp_ny >= market_open_ny_today:
#         return (timestamp_ny - market_open_ny_today).total_seconds() // 60
#     return 0.0

# # Scalar UDF for market open spike flag
# @udf(result_type=DataTypes.INT())
# def market_open_spike_flag_udf(timestamp_utc: datetime) -> int:
#     """Returns 1 if timestamp is within market open spike window (9:30-9:34 AM NY time)."""
#     timestamp_ny = timestamp_utc.astimezone(NY_TIMEZONE)
#     market_open_time = timestamp_ny.replace(hour=9, minute=30, second=0, microsecond=0).time()
#     market_open_end_time = timestamp_ny.replace(hour=9, minute=34, second=0, microsecond=0).time()
#     return 1 if market_open_time <= timestamp_ny.time() <= market_open_end_time else 0

# # Scalar UDF for market close spike flag
# @udf(result_type=DataTypes.INT())
# def market_close_spike_flag_udf(timestamp_utc: datetime) -> int:
#     """Returns 1 if timestamp is within market close spike window (3:56-4:00 PM NY time)."""
#     timestamp_ny = timestamp_utc.astimezone(NY_TIMEZONE)
#     market_close_start_time = timestamp_ny.replace(hour=15, minute=56, second=0, microsecond=0).time()
#     market_close_time = timestamp_ny.replace(hour=16, minute=0, second=0, microsecond=0).time()
#     return 1 if market_close_start_time <= timestamp_ny.time() <= market_close_time else 0

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#     t_env = StreamTableEnvironment.create(env)

#     # Registra le UDF
#     t_env.create_temporary_system_function("minutes_since_market_open_udf", minutes_since_market_open_udf)
#     t_env.create_temporary_system_function("market_open_spike_flag_udf", market_open_spike_flag_udf)
#     t_env.create_temporary_system_function("market_close_spike_flag_udf", market_close_spike_flag_udf)

#     # Configurazione Kafka
#     kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
#     postgres_host = os.getenv("POSTGRES_HOST", "postgre")
#     postgres_port = os.getenv("POSTGRES_PORT", "5432")
#     postgres_db = os.getenv("POSTGRES_DB", "aggregated-data")
#     postgres_user = os.getenv("POSTGRES_USER", "admin")
#     postgres_password = os.getenv("POSTGRES_PASSWORD", "admin123")

#     # 1. Registra la tabella per i dati azionari
#     # AGGIORNAMENTO QUI: Includere tutti i campi da Alpaca e usare backticks
#     t_env.execute_sql(f"""
#             CREATE TABLE stock_data (
#                 ticker STRING,
#                 `data` ARRAY<ROW<
#                     `timestamp` STRING,
#                     `open` DOUBLE,
#                     `high` DOUBLE,
#                     `low` DOUBLE,
#                     `close` DOUBLE,
#                     `volume` DOUBLE,
#                     `trade_count` BIGINT,
#                     `vwap` DOUBLE
#                 >>,
#                 timestamp_parsed AS CAST(`data`[1].`timestamp` AS TIMESTAMP(3)),
#                 WATERMARK FOR timestamp_parsed AS timestamp_parsed
#             ) WITH (
#                 'connector' = 'kafka',
#                 'topic' = 'h_alpaca',
#                 'properties.bootstrap.servers' = '{kafka_brokers}',
#                 'properties.group.id' = 'flink_sql_stock_consumer',
#                 'scan.startup.mode' = 'earliest-offset',
#                 'format' = 'json',
#                 'json.fail-on-missing-field' = 'false',
#                 'json.map-null-keys.mode' = 'DROP',
#                 'json.timestamp-format.standard' = 'ISO-8601'
#             )
#         """)

#     # 2. Registra la tabella per i dati macro
#     t_env.execute_sql(f"""
#         CREATE TABLE macro_data_raw (
#             series STRING,
#             date_str STRING,
#             value DOUBLE,
#             ts AS TO_TIMESTAMP(date_str, 'yyyy-MM-dd'),
#             WATERMARK FOR ts AS ts - INTERVAL '1' DAY
#         ) WITH (
#             'connector' = 'kafka',
#             'topic' = 'h_macrodata',
#             'properties.bootstrap.servers' = '{kafka_brokers}',
#             'properties.group.id' = 'flink_sql_macro_consumer',
#             'scan.startup.mode' = 'earliest-offset',
#             'format' = 'json',
#             'json.fail-on-missing-field' = 'false'
#         )
#     """)

#     # 3. Definisci la vista temporale per i dati macro
#     t_env.execute_sql("""
#         CREATE VIEW macro_data_temporal AS
#         SELECT
#             series,
#             value,
#             ts
#         FROM
#             macro_data_raw
#         QUALIFY ROW_NUMBER() OVER (PARTITION BY series ORDER BY ts DESC) = 1
#     """)

#     # 4. Registra la tabella PostgreSQL come sink
#     t_env.execute_sql(f"""
#         CREATE TABLE aggregated_stock_data_sink (
#             ticker VARCHAR(10),
#             timestamp TIMESTAMP(3),
#             price_mean_1min DOUBLE,
#             price_mean_5min DOUBLE,
#             price_std_5min DOUBLE,
#             price_mean_30min DOUBLE,
#             price_std_30min DOUBLE,
#             size_tot_1min DOUBLE,
#             size_tot_5min DOUBLE,
#             size_tot_30min DOUBLE,
#             sentiment_bluesky_mean_2hours DOUBLE,
#             sentiment_bluesky_mean_1day DOUBLE,
#             sentiment_news_mean_1day DOUBLE,
#             sentiment_news_mean_3days DOUBLE,
#             sentiment_general_bluesky_mean_2hours DOUBLE,
#             sentiment_general_bluesky_mean_1day DOUBLE,
#             minutes_since_open DOUBLE,
#             day_of_week INTEGER,
#             day_of_month INTEGER,
#             week_of_year INTEGER,
#             month_of_year INTEGER,
#             market_open_spike_flag INTEGER,
#             market_close_spike_flag INTEGER,
#             gdp_real DOUBLE,
#             cpi DOUBLE,
#             ffr DOUBLE,
#             t10y DOUBLE,
#             t2y DOUBLE,
#             spread_10y_2y DOUBLE,
#             unemployment DOUBLE,
#             y1 DOUBLE
#         ) WITH (
#             'connector' = 'jdbc',
#             'url' = 'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}',
#             'table-name' = 'aggregated_stock_data',
#             'username' = '{postgres_user}',
#             'password' = '{postgres_password}',
#             'sink.buffer-flush.max-rows' = '1',
#             'sink.buffer-flush.interval' = '1s'
#         )
#     """)

#     # 5. Esegui la logica di aggregazione e join utilizzando Flink SQL
#     # AGGIORNAMENTO QUI: UNNEST deve estrarre tutti i campi definiti nella tabella stock_data
#     transformed_data_table = t_env.sql_query(f"""
#     SELECT
#         s.ticker,
#         s_data.`timestamp` AS timestamp, -- Usa il timestamp dal record s_data
#         s_data.`open` AS price_mean_1min,
#         s_data.`open` AS price_mean_5min,
#         0.0 AS price_std_5min,
#         s_data.`open` AS price_mean_30min,
#         0.0 AS price_std_30min,
#         s_data.`volume` AS size_tot_1min,
#         s_data.`volume` AS size_tot_5min,
#         s_data.`volume` AS size_tot_30min,
#         0.0 AS sentiment_bluesky_mean_2hours,
#         0.0 AS sentiment_bluesky_mean_1day,
#         0.0 AS sentiment_news_mean_1day,
#         0.0 AS sentiment_news_mean_3days,
#         0.0 AS sentiment_general_bluesky_mean_2hours,
#         0.0 AS sentiment_general_bluesky_mean_1day,
#         minutes_since_market_open_udf(s_data.`timestamp`) AS minutes_since_open,
#         EXTRACT(DAYOFWEEK FROM s_data.`timestamp`) AS day_of_week,
#         EXTRACT(DAY FROM s_data.`timestamp`) AS day_of_month,
#         EXTRACT(WEEK FROM s_data.`timestamp`) AS week_of_year,
#         EXTRACT(MONTH FROM s_data.`timestamp`) AS month_of_year,
#         market_open_spike_flag_udf(s_data.`timestamp`) AS market_open_spike_flag,
#         market_close_spike_flag_udf(s_data.`timestamp`) AS market_close_spike_flag,
#         COALESCE(gdp.value, 0.0) AS gdp_real,
#         COALESCE(cpi.value, 0.0) AS cpi,
#         COALESCE(ffr.value, 0.0) AS ffr,
#         COALESCE(t10y.value, 0.0) AS t10y,
#         COALESCE(t2y.value, 0.0) AS t2y,
#         COALESCE(spread.value, 0.0) AS spread_10y_2y,
#         COALESCE(unemp.value, 0.0) AS unemployment,
#         s_data.`close` AS y1
#     FROM
#         stock_data AS s,
#         -- AGGIORNAMENTO QUI: UNNEST deve estrarre tutti i campi
#         UNNEST(s.`data`) AS s_data(`timestamp`, `open`, `high`, `low`, `close`, `volume`, `trade_count`, `vwap`)
#     LEFT JOIN macro_data_temporal FOR SYSTEM_TIME AS OF s_data.`timestamp` AS gdp
#         ON gdp.series = 'gdp_real'
#     LEFT JOIN macro_data_temporal FOR SYSTEM_TIME AS OF s_data.`timestamp` AS cpi
#         ON cpi.series = 'cpi'
#     LEFT JOIN macro_data_temporal FOR SYSTEM_TIME AS OF s_data.`timestamp` AS ffr
#         ON ffr.series = 'ffr'
#     LEFT JOIN macro_data_temporal FOR SYSTEM_TIME AS OF s_data.`timestamp` AS t10y
#         ON t10y.series = 't10y'
#     LEFT JOIN macro_data_temporal FOR SYSTEM_TIME AS OF s_data.`timestamp` AS t2y
#         ON t2y.series = 't2y'
#     LEFT JOIN macro_data_temporal FOR SYSTEM_TIME AS OF s_data.`timestamp` AS spread
#         ON spread.series = 'spread_10y_2y'
#     LEFT JOIN macro_data_temporal FOR SYSTEM_TIME AS OF s_data.`timestamp` AS unemp
#         ON unemp.series = 'unemployment'
#     """)

#     # --- BLOCCO PRINT SINK (PER DEBUGGING) ---
#     print("\n[INFO] Starting Print Sink for debugging. Look for output in Flink TaskManager logs (or console).\n")
#     transformed_data_table.execute().print()
#     print("\n[INFO] Print Sink job started. Data will appear as it's processed.\n")
#     # --- FINE BLOCCO PRINT SINK ---

#     # Produzione: Inserisci i risultati nel sink PostgreSQL
#     print("[INFO] Starting PostgreSQL Sink job.")
#     transformed_data_table.execute_insert("aggregated_stock_data_sink").wait()
#     print("[INFO] PostgreSQL Sink job finished.")

# if __name__ == "__main__":
#     main()
