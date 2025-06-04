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

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# macro_alias = {
#     "GDPC1": "gdp_real",
#     "CPIAUCSL": "cpi",
#     "FEDFUNDS": "ffr",
#     "DGS10": "t10y",
#     "DGS2": "t2y",
#     "T10Y2Y": "spread_10y_2y",
#     "UNRATE": "unemployment"
# }

# GENERAL_SENTIMENT_KEY = "general_sentiment_key"

# macro_data_dict = {}
# general_sentiment_dict = {
#     "sentiment_bluesky_mean_general_2hours": 0.0,
#     "sentiment_bluesky_mean_general_1d": 0.0
# }
# fundamentals_data = {}

# NY_TZ = pytz.timezone('America/New_York')

# MINIO_URL = "minio:9000"
# MINIO_ACCESS_KEY = "admin"
# MINIO_SECRET_KEY = "admin123"
# MINIO_SECURE = False

# def load_fundamental_data():
#     print(" [INIT] Loading fundamental data from MinIO...", file=sys.stderr)
#     try:
#         minio_client = Minio(
#             MINIO_URL,
#             access_key=MINIO_ACCESS_KEY,
#             secret_key=MINIO_SECRET_KEY,
#             secure=MINIO_SECURE
#         )
        
#         bucket_name = "company-fundamentals"
        
#         if not minio_client.bucket_exists(bucket_name):
#             print(f"[ERROR] MinIO bucket '{bucket_name}' does not exist. No fundamental data loaded.", file=sys.stderr)
#             return

#         for ticker in TOP_30_TICKERS:
#             object_name = f"{ticker}/2024.parquet"
#             response = None
#             try:
#                 response = minio_client.get_object(bucket_name, object_name)
                
#                 parquet_bytes = io.BytesIO(response.read())
#                 parquet_bytes.seek(0)
#                 df = pd.read_parquet(parquet_bytes)
                
#                 if not df.empty:
#                     row = df.iloc[0]
#                     # Ensure all values are converted to standard Python types (float, int)
#                     eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) else None
#                     fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) else None
#                     revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) else None
#                     net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) else None
#                     debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) else None
#                     equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) else None

#                     profit_margin = net_income / revenue if revenue is not None and revenue != 0 else None
#                     debt_to_equity = debt / equity if equity is not None and equity != 0 else None

#                     fundamentals_data[ticker] = {
#                         "eps": eps,
#                         "freeCashFlow": fcf,
#                         "profit_margin": profit_margin,
#                         "debt_to_equity": debt_to_equity
#                     }
#                     print(f"[FUNDAMENTALS] Loaded data for {ticker}: {fundamentals_data[ticker]}", file=sys.stderr)
#                 else:
#                     print(f"[WARN] Parquet file for {ticker}/{object_name} is empty.", file=sys.stderr)

#             except S3Error as e:
#                 print(f"[ERROR] MinIO S3 Error for {ticker} ({object_name}): {e}", file=sys.stderr)
#             except Exception as e:
#                 print(f"[ERROR] Could not load fundamental data for {ticker} from MinIO ({object_name}): {e}", file=sys.stderr)
#             finally:
#                 if response:
#                     response.close()
#                     response.release_conn()
#         print(" [INIT] Fundamental data loading complete.", file=sys.stderr)

#     except Exception as e:
#         print(f"[CRITICAL] Failed to initialize Minio client or load any fundamental data: {e}", file=sys.stderr)

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

#         # States for sentiment (no real/fake distinction needed)
#         self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
#         self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

#         self.sentiment_bluesky_general_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_general_2h"))
#         self.sentiment_bluesky_general_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_general_1d"))

#         self.last_timer_state = runtime_context.get_state(
#             ValueStateDescriptor("last_timer", Types.LONG()))


#     def _cleanup_old_entries(self, state, window_minutes):
#         """Removes entries from state that are older than the specified window_minutes."""
#         threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
#         keys_to_remove = []
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


#     def process_element(self, value, ctx):
#         """Processes each incoming element (JSON string) from Kafka."""
#         try:
#             data = json.loads(value)
#             current_key = ctx.get_current_key()

#             # --- Handle Macro Data ---
#             if current_key == "macro_data_key":
#                 alias_key = data.get("alias")
#                 if alias_key:
#                     macro_data_dict[alias_key] = float(data["value"])
#                     print(f"[MACRO] {alias_key}: {macro_data_dict[alias_key]}", file=sys.stderr)
#                 return []

#             # --- Handle Sentiment Data (specific tickers or GENERAL) ---
#             elif "social" in data and "sentiment_score" in data:
#                 social_source = data.get("social")
#                 sentiment_score = float(data.get("sentiment_score"))
#                 ts_str = data.get("timestamp")
                
#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in sentiment data: {data}", file=sys.stderr)
#                     return []

#                 if current_key == GENERAL_SENTIMENT_KEY:
#                     if social_source == "bluesky":
#                         self.sentiment_bluesky_general_2h.put(ts_str, sentiment_score)
#                         self.sentiment_bluesky_general_1d.put(ts_str, sentiment_score)
#                         print(f"[SENTIMENT-GENERAL] Bluesky - {ts_str}: {sentiment_score}", file=sys.stderr)
#                     else:
#                         print(f"[WARN] General sentiment received for non-Bluesky source: {social_source}", file=sys.stderr)
#                     return []
                
#                 elif current_key in TOP_30_TICKERS:
#                     if social_source == "bluesky":
#                         self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
#                         self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
#                     elif social_source == "news":
#                         self.sentiment_news_1d.put(ts_str, sentiment_score)
#                         self.sentiment_news_3d.put(ts_str, sentiment_score)
#                     else:
#                         print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
#                         return []
#                     print(f"[SENTIMENT] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr)
#                 return []

#             # --- Handle Stock Trade Data ---
#             elif "price" in data and "size" in data and "exchange" in data: # Added 'exchange' check
#                 ticker = data.get("ticker")
#                 if ticker not in TOP_30_TICKERS:
#                     return []
                
#                 ts_str = data.get("timestamp")
#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
#                     return []

#                 price = float(data.get("price"))
#                 size = float(data.get("size"))
#                 exchange = data.get("exchange") # Retrieve exchange type

#                 if exchange != "RANDOM": # Real data (e.g., "V" for Virtu Financial)
#                     self.real_price_1m.put(ts_str, price)
#                     self.real_price_5m.put(ts_str, price)
#                     self.real_price_30m.put(ts_str, price)
#                     self.real_size_1m.put(ts_str, size)
#                     self.real_size_5m.put(ts_str, size)
#                     self.real_size_30m.put(ts_str, size)
#                     # print(f"[TRADE-REAL] {ticker} - {ts_str}: Price={price}, Size={size}", file=sys.stderr) # Removed verbose print
#                 else: # Fake (simulated) data
#                     self.fake_price_1m.put(ts_str, price)
#                     self.fake_price_5m.put(ts_str, price)
#                     self.fake_price_30m.put(ts_str, price)
#                     self.fake_size_1m.put(ts_str, size)
#                     self.fake_size_5m.put(ts_str, size)
#                     self.fake_size_30m.put(ts_str, size)
#                     # print(f"[TRADE-FAKE] {ticker} - {ts_str}: Price={price}, Size={size}", file=sys.stderr) # Removed verbose print
#                 return []

#             else:
#                 print(f"[WARN] Unrecognized data format: {value}", file=sys.stderr)
#                 return []

#         except json.JSONDecodeError:
#             print(f"[ERROR] Failed to decode JSON: {value}", file=sys.stderr)
#             return []
#         except Exception as e:
#             print(f"[ERROR] process_element: {e} for value: {value}", file=sys.stderr)
#             return []
#         finally:
#             # Register timer for predictions every 5 seconds
#             last_timer = self.last_timer_state.value()
#             if last_timer is None or ctx.timer_service().current_processing_time() >= last_timer:
#                 next_ts = ctx.timer_service().current_processing_time() + 5000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)

#             # Removed market open cleanup timer specific logic, as handling is now different.


#     def on_timer(self, timestamp, ctx):
#         """Called when a registered timer fires."""
#         try:
#             now_utc = datetime.now(timezone.utc)
#             ts_str = now_utc.isoformat()
#             ticker = ctx.get_current_key()

#             # Helper functions (remain the same)
#             def mean(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else 0.0

#             def std(vals):
#                 vals = list(vals)
#                 return float(np.std(vals)) if vals and len(vals) > 1 else 0.0

#             def total(vals):
#                 vals = list(vals)
#                 return float(np.sum(vals)) if vals else 0.0

#             # --- Handle General Sentiment Key ---
#             if ticker == GENERAL_SENTIMENT_KEY:
#                 self._cleanup_old_entries(self.sentiment_bluesky_general_2h, 2 * 60)
#                 self._cleanup_old_entries(self.sentiment_bluesky_general_1d, 24 * 60)

#                 general_sentiment_dict["sentiment_bluesky_mean_general_2hours"] = mean(self.sentiment_bluesky_general_2h.values())
#                 general_sentiment_dict["sentiment_bluesky_mean_general_1d"] = mean(self.sentiment_bluesky_general_1d.values())
                
#                 print(f"[GENERAL SENTIMENT AGG] Updated general_sentiment_dict: {general_sentiment_dict}", file=sys.stderr)
#                 return []

#             # --- Handle Macro Data Key ---
#             if ticker == "macro_data_key":
#                 return []

#             # --- Handle Specific Ticker Data (TOP_30_TICKERS) ---
#             if ticker not in TOP_30_TICKERS:
#                 print(f"[WARN] on_timer fired for unexpected key: {ticker}", file=sys.stderr)
#                 return []

#             # Cleanup for ALL states (real and fake)
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

#             # Sentiment cleanup (unchanged)
#             self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
#             self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)

#             now_ny = now_utc.astimezone(NY_TZ)
#             market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#             market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
            
#             is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5 # Mon-Fri

#             is_simulated_prediction = False
#             if is_market_hours:
#                 # If market is open, use real data
#                 # print(f"[PREDICTION] Market is open for {ticker}. Using REAL data.", file=sys.stderr) # Removed verbose print
#                 price_1m_values = self.real_price_1m.values()
#                 price_5m_values = self.real_price_5m.values()
#                 price_30m_values = self.real_price_30m.values()
#                 size_1m_values = self.real_size_1m.values()
#                 size_5m_values = self.real_size_5m.values()
#                 size_30m_values = self.real_size_30m.values()
#                 is_simulated_prediction = False
#             else:
#                 # If market is closed, use fake data for prediction (Option B)
#                 # print(f"[PREDICTION] Market is closed for {ticker}. Using FAKE data for simulation.", file=sys.stderr) # Removed verbose print
#                 price_1m_values = self.fake_price_1m.values()
#                 price_5m_values = self.fake_price_5m.values()
#                 price_30m_values = self.fake_price_30m.values()
#                 size_1m_values = self.fake_size_1m.values()
#                 size_5m_values = self.fake_size_5m.values()
#                 size_30m_values = self.fake_size_30m.values()
#                 is_simulated_prediction = True


#             # Calculate market open/close spike flags (independent of prediction generation)
#             market_open_spike_flag = 0
#             market_close_spike_flag = 0

#             if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
#                 market_open_spike_flag = 1
            
#             if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
#                 market_close_spike_flag = 1

#             ticker_fundamentals = fundamentals_data.get(ticker, {})

#             # Handle "minutes_since_open" for dynamic values
#             minutes_since_open = -1 # Default value
#             if now_ny >= market_open_time and now_ny < market_close_time:
#                 minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
#             else:
#                 # If before market open (e.g., overnight), calculate minutes from previous day's open
#                 # otherwise, minutes from close
#                 if now_ny < market_open_time: # From midnight until market open
#                     minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
#                     minutes_since_open = -(minutes_until_open) # Negative value indicates "before open"
#                 else: # After market close
#                     minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + (16*60 - 9*60 - 30) # Minutes from close + market duration


#             features = {
#                 "ticker": ticker,
#                 "timestamp": ts_str,
#                 "price_mean_1min": mean(price_1m_values),
#                 "price_mean_5min": mean(price_5m_values),
#                 "price_std_5min": std(price_5m_values),
#                 "price_mean_30min": mean(price_30m_values),
#                 "price_std_30min": std(price_30m_values),
#                 "size_tot_1min": total(size_1m_values),
#                 "size_tot_5min": total(size_5m_values),
#                 "size_tot_30min": total(size_30m_values),
#                 # SENTIMENT
#                 "sentiment_bluesky_mean_2h": mean(self.sentiment_bluesky_2h.values()),
#                 "sentiment_bluesky_mean_1d": mean(self.sentiment_bluesky_1d.values()),
#                 "sentiment_news_mean_1d": mean(self.sentiment_news_1d.values()),
#                 "sentiment_news_mean_3d": mean(self.sentiment_news_3d.values()),
#                 "sentiment_bluesky_mean_general_2hours": general_sentiment_dict["sentiment_bluesky_mean_general_2hours"],
#                 "sentiment_bluesky_mean_general_1d": general_sentiment_dict["sentiment_bluesky_mean_general_1d"],
#                 # NEW TIME-BASED FEATURES
#                 "minutes_since_open": int(minutes_since_open),
#                 "day_of_week": int(now_ny.weekday()),
#                 "day_of_month": int(now_ny.day),
#                 "week_of_year": int(now_ny.isocalendar()[1]),
#                 "month_of_year": int(now_ny.month),
#                 "market_open_spike_flag": int(market_open_spike_flag),
#                 "market_close_spike_flag": int(market_close_spike_flag),
#                 # Fundamental data
#                 "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
#                 "freeCashFlow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
#                 "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
#                 "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None,
#                 # Flag to indicate if the prediction is based on simulated data
#                 "is_simulated_prediction": is_simulated_prediction
#             }

#             for macro_key_alias, macro_value in macro_data_dict.items():
#                 features[macro_key_alias] = float(macro_value)

#             result = json.dumps(features)
#             print(f"[PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             return [result]
#         except Exception as e:
#             print(f"[ERROR] on_timer for ticker {ticker}: {e}", file=sys.stderr)
#             return [json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)})]

# # --- Helper for splitting sentiment data ---
# def expand_sentiment_data(json_str):
#     """
#     Expands a single sentiment JSON string into multiple if it contains a list of tickers,
#     otherwise passes through other data types.
#     Handles 'GENERAL' ticker by passing it through.
#     """
#     try:
#         data = json.loads(json_str)
        
#         if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
#             expanded_records = []
#             original_ticker_list = data["ticker"]
            
#             if "GENERAL" in original_ticker_list:
#                 new_record = data.copy()
#                 new_record["ticker"] = "GENERAL"
#                 expanded_records.append(json.dumps(new_record))
#                 print(f"[WARN] Sentiment data with 'GENERAL' ticker detected and handled.", file=sys.stderr)
            
#             for ticker_item in original_ticker_list:
#                 if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
#                     new_record = data.copy()
#                     new_record["ticker"] = ticker_item
#                     expanded_records.append(json.dumps(new_record))
            
#             if not expanded_records:
#                 print(f"[WARN] Sentiment data with no tracked or 'GENERAL' tickers: {json.loads(json_str).get('ticker')}", file=sys.stderr)
#                 return []
#             return expanded_records
        
#         return [json_str]
#     except json.JSONDecodeError:
#         print(f"[ERROR] Failed to decode JSON in expand_sentiment_data: {json_str}", file=sys.stderr)
#         return []
#     except Exception as e:
#         print(f"[ERROR] expand_sentiment_data: {e} for {json_str}", file=sys.stderr)
#         return []

# def route_by_ticker(json_str):
#     """Determines the key for incoming JSON data."""
#     try:
#         data = json.loads(json_str)
#         if "alias" in data:
#             return "macro_data_key"
#         elif "ticker" in data:
#             if data["ticker"] == "GENERAL":
#                 return GENERAL_SENTIMENT_KEY
#             elif data["ticker"] in TOP_30_TICKERS:
#                 return data["ticker"]
#             else:
#                 print(f"[WARN] Ticker '{data['ticker']}' not in TOP_30_TICKERS. Discarding.", file=sys.stderr)
#                 return "discard_key"
#         else:
#             print(f"[WARN] Data with no 'alias' or 'ticker' field: {json_str}", file=sys.stderr)
#             return "unknown_data_key"
#     except json.JSONDecodeError:
#         print(f"[WARN] Failed to decode JSON for key_by: {json_str}", file=sys.stderr)
#         return "invalid_json_key"
#     except Exception as e:
#         print(f"[ERROR] route_by_ticker: {e} for {json_str}", file=sys.stderr)
#         return "error_key"


# def main():
#     load_fundamental_data()

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1) # We keep parallelism at 1 for now to avoid issues
#                             # with global dicts and data consistency.

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_group',
#         'auto.offset.reset': 'earliest'
#     }

#     consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "macrodata", "news_sentiment", "bluesky_sentiment"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregated_data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
    
#     expanded_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())

#     keyed = expanded_stream.key_by(route_by_ticker, key_type=Types.STRING())
    
#     processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
    
#     processed.add_sink(producer)

#     env.execute("Full Aggregation with Sliding Windows, Macrodata, Sentiment, Time, and Fundamental Features (Continuous)")

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

TOP_30_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

macro_alias = {
    "GDPC1": "gdp_real",
    "CPIAUCSL": "cpi",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment"
}

GENERAL_SENTIMENT_KEY = "general_sentiment_key"

macro_data_dict = {}
general_sentiment_dict = {
    "sentiment_bluesky_mean_general_2hours": 0.0,
    "sentiment_bluesky_mean_general_1d": 0.0
}
fundamentals_data = {}

NY_TZ = pytz.timezone('America/New_York')

MINIO_URL = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_SECURE = False

def load_fundamental_data():
    print(" [INIT] Loading fundamental data from MinIO...", file=sys.stderr)
    try:
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        
        bucket_name = "company-fundamentals"
        
        if not minio_client.bucket_exists(bucket_name):
            print(f"[ERROR] MinIO bucket '{bucket_name}' does not exist. No fundamental data loaded.", file=sys.stderr)
            return

        for ticker in TOP_30_TICKERS:
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
                    eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) else None
                    fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) else None
                    revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) else None
                    net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) else None
                    debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) else None
                    equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) else None

                    profit_margin = net_income / revenue if revenue is not None and revenue != 0 else None
                    debt_to_equity = debt / equity if equity is not None and equity != 0 else None

                    fundamentals_data[ticker] = {
                        "eps": eps,
                        "freeCashFlow": fcf,
                        "profit_margin": profit_margin,
                        "debt_to_equity": debt_to_equity
                    }
                    print(f"[FUNDAMENTALS] Loaded data for {ticker}: {fundamentals_data[ticker]}", file=sys.stderr)
                else:
                    print(f"[WARN] Parquet file for {ticker}/{object_name} is empty.", file=sys.stderr)

            except S3Error as e:
                print(f"[ERROR] MinIO S3 Error for {ticker} ({object_name}): {e}", file=sys.stderr)
            except Exception as e:
                print(f"[ERROR] Could not load fundamental data for {ticker} from MinIO ({object_name}): {e}", file=sys.stderr)
            finally:
                if response:
                    response.close()
                    response.release_conn()
        print(" [INIT] Fundamental data loading complete.", file=sys.stderr)

    except Exception as e:
        print(f"[CRITICAL] Failed to initialize Minio client or load any fundamental data: {e}", file=sys.stderr)

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

        # States for sentiment (no real/fake distinction needed)
        self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
        self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
        self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
        self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

        self.sentiment_bluesky_general_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_general_2h"))
        self.sentiment_bluesky_general_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_general_1d"))

        self.last_timer_state = runtime_context.get_state(
            ValueStateDescriptor("last_timer", Types.LONG()))


    def _cleanup_old_entries(self, state, window_minutes):
        """Removes entries from state that are older than the specified window_minutes."""
        threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        keys_to_remove = []
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


    def process_element(self, value, ctx):
        """Processes each incoming element (JSON string) from Kafka."""
        try:
            data = json.loads(value)
            current_key = ctx.get_current_key()

            # --- Handle Macro Data ---
            if current_key == "macro_data_key":
                alias_key = data.get("alias")
                if alias_key:
                    macro_data_dict[alias_key] = float(data["value"])
                    print(f"[MACRO] {alias_key}: {macro_data_dict[alias_key]}", file=sys.stderr)
                return []

            # --- Handle Sentiment Data (specific tickers or GENERAL) ---
            elif "social" in data and "sentiment_score" in data:
                social_source = data.get("social")
                sentiment_score = float(data.get("sentiment_score"))
                ts_str = data.get("timestamp")
                
                if not ts_str:
                    print(f"[ERROR] Missing timestamp in sentiment data: {data}", file=sys.stderr)
                    return []

                if current_key == GENERAL_SENTIMENT_KEY:
                    if social_source == "bluesky":
                        self.sentiment_bluesky_general_2h.put(ts_str, sentiment_score)
                        self.sentiment_bluesky_general_1d.put(ts_str, sentiment_score)
                        print(f"[SENTIMENT-GENERAL] Bluesky - {ts_str}: {sentiment_score}", file=sys.stderr)
                    else:
                        print(f"[WARN] General sentiment received for non-Bluesky source: {social_source}", file=sys.stderr)
                    return []
                
                elif current_key in TOP_30_TICKERS:
                    if social_source == "bluesky":
                        self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
                        self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
                    elif social_source == "news":
                        self.sentiment_news_1d.put(ts_str, sentiment_score)
                        self.sentiment_news_3d.put(ts_str, sentiment_score)
                    else:
                        print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
                        return []
                    print(f"[SENTIMENT] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr)
                return []

            # --- Handle Stock Trade Data ---
            elif "price" in data and "size" in data and "exchange" in data: # Added 'exchange' check
                ticker = data.get("ticker")
                if ticker not in TOP_30_TICKERS:
                    return []
                
                ts_str = data.get("timestamp")
                if not ts_str:
                    print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
                    return []

                price = float(data.get("price"))
                size = float(data.get("size"))
                exchange = data.get("exchange") # Retrieve exchange type

                if exchange != "RANDOM": # Real data (e.g., "V" for Virtu Financial)
                    self.real_price_1m.put(ts_str, price)
                    self.real_price_5m.put(ts_str, price)
                    self.real_price_30m.put(ts_str, price)
                    self.real_size_1m.put(ts_str, size)
                    self.real_size_5m.put(ts_str, size)
                    self.real_size_30m.put(ts_str, size)
                    # print(f"[TRADE-REAL] {ticker} - {ts_str}: Price={price}, Size={size}", file=sys.stderr) # Removed verbose print
                else: # Fake (simulated) data
                    self.fake_price_1m.put(ts_str, price)
                    self.fake_price_5m.put(ts_str, price)
                    self.fake_price_30m.put(ts_str, price)
                    self.fake_size_1m.put(ts_str, size)
                    self.fake_size_5m.put(ts_str, size)
                    self.fake_size_30m.put(ts_str, size)
                    # print(f"[TRADE-FAKE] {ticker} - {ts_str}: Price={price}, Size={size}", file=sys.stderr) # Removed verbose print
                return []

            else:
                print(f"[WARN] Unrecognized data format: {value}", file=sys.stderr)
                return []

        except json.JSONDecodeError:
            print(f"[ERROR] Failed to decode JSON: {value}", file=sys.stderr)
            return []
        except Exception as e:
            print(f"[ERROR] process_element: {e} for value: {value}", file=sys.stderr)
            return []
        finally:
            # Register timer for predictions every 10 seconds, aligned to the clock
            current_processing_time = ctx.timer_service().current_processing_time() # in milliseconds
            
            # Convert to seconds and find the next multiple of 10
            current_seconds = current_processing_time // 1000
            next_timer_seconds = (current_seconds // 10 + 1) * 10
            
            # If the calculated next_timer_seconds is the same as current_seconds, 
            # it means we are exactly on a 10-second mark, so the next timer should be 
            # 10 seconds from now. This handles the edge case where current_processing_time
            # is exactly at a 10-second boundary.
            if next_timer_seconds * 1000 == current_processing_time:
                next_timer_seconds += 10 # Move to the next 10-second mark

            next_timer_timestamp = next_timer_seconds * 1000 # convert back to milliseconds

            last_timer = self.last_timer_state.value()

            # Register timer only if it's the first timer for this key, or if the calculated
            # next timer is later than the last registered timer. This prevents registering
            # multiple timers for the same future timestamp.
            if last_timer is None or next_timer_timestamp > last_timer:
                ctx.timer_service().register_processing_time_timer(next_timer_timestamp)
                self.last_timer_state.update(next_timer_timestamp)
                print(f"[TIMER] Registered timer for {ctx.get_current_key()} at {datetime.fromtimestamp(next_timer_timestamp / 1000, tz=timezone.utc).isoformat()}", file=sys.stderr)

    def on_timer(self, timestamp, ctx):
        """Called when a registered timer fires."""
        try:
            now_utc = datetime.now(timezone.utc)
            ts_str = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).isoformat()
            ticker = ctx.get_current_key()

            # Helper functions (le modifichiamo leggermente per essere più esplicite)
            def mean(vals_list): # Ora prendono direttamente la lista
                return float(np.mean(vals_list)) if vals_list else 0.0

            def std(vals_list): # Ora prendono direttamente la lista
                return float(np.std(vals_list)) if vals_list and len(vals_list) > 1 else 0.0 # Mantenuto > 1 per la std

            def total(vals_list): # Ora prendono direttamente la lista
                return float(np.sum(vals_list)) if vals_list else 0.0

            # --- Handle General Sentiment Key ---
            if ticker == GENERAL_SENTIMENT_KEY:
                self._cleanup_old_entries(self.sentiment_bluesky_general_2h, 2 * 60)
                self._cleanup_old_entries(self.sentiment_bluesky_general_1d, 24 * 60)

                # Converti in lista una volta
                bluesky_gen_2h_list = list(self.sentiment_bluesky_general_2h.values())
                bluesky_gen_1d_list = list(self.sentiment_bluesky_general_1d.values())

                general_sentiment_dict["sentiment_bluesky_mean_general_2hours"] = mean(bluesky_gen_2h_list)
                general_sentiment_dict["sentiment_bluesky_mean_general_1d"] = mean(bluesky_gen_1d_list)
                
                print(f"[GENERAL SENTIMENT AGG] Updated general_sentiment_dict: {general_sentiment_dict}", file=sys.stderr)
                # ... (re-register timer remain unchanged)
                return []

            # --- Handle Macro Data Key (unchanged, as it doesn't use these aggregates) ---
            if ticker == "macro_data_key":
                return []

            # --- Handle Specific Ticker Data (TOP_30_TICKERS) ---
            if ticker not in TOP_30_TICKERS:
                print(f"[WARN] on_timer fired for unexpected key: {ticker}", file=sys.stderr)
                return []

            # Cleanup for ALL states (real and fake) - unchanged
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

            # Sentiment cleanup - unchanged
            self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
            self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
            self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
            self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)

            now_ny = now_utc.astimezone(NY_TZ)
            market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
            
            is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5 # Mon-Fri

            is_simulated_prediction = False
            
            # --- CONVERTI GLI ITERATORI IN LISTE UNA SOLA VOLTA QUI ---
            if is_market_hours:
                price_1m_list = list(self.real_price_1m.values())
                price_5m_list = list(self.real_price_5m.values())
                price_30m_list = list(self.real_price_30m.values())
                size_1m_list = list(self.real_size_1m.values())
                size_5m_list = list(self.real_size_5m.values())
                size_30m_list = list(self.real_size_30m.values())
                is_simulated_prediction = False
            else:
                price_1m_list = list(self.fake_price_1m.values())
                price_5m_list = list(self.fake_price_5m.values())
                price_30m_list = list(self.fake_price_30m.values())
                size_1m_list = list(self.fake_size_1m.values())
                size_5m_list = list(self.fake_size_5m.values())
                size_30m_list = list(self.fake_size_30m.values())
                is_simulated_prediction = True

            # Converti anche i valori di sentiment in liste una sola volta
            sentiment_bluesky_2h_list = list(self.sentiment_bluesky_2h.values())
            sentiment_bluesky_1d_list = list(self.sentiment_bluesky_1d.values())
            sentiment_news_1d_list = list(self.sentiment_news_1d.values())
            sentiment_news_3d_list = list(self.sentiment_news_3d.values())

            # Calculate market open/close spike flags (independent of prediction generation) - unchanged
            market_open_spike_flag = 0
            market_close_spike_flag = 0

            if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
                market_open_spike_flag = 1
            
            if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
                market_close_spike_flag = 1

            ticker_fundamentals = fundamentals_data.get(ticker, {})

            # Handle "minutes_since_open" for dynamic values - unchanged
            minutes_since_open = -1 # Default value
            if now_ny >= market_open_time and now_ny < market_close_time:
                minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
            else:
                if now_ny < market_open_time:
                    minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
                    minutes_since_open = -(minutes_until_open)
                else:
                    minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + (16*60 - 9*60 - 30)


            features = {
                "ticker": ticker,
                "timestamp": ts_str,
                "price_mean_1min": mean(price_1m_list),
                "price_mean_5min": mean(price_5m_list),
                "price_std_5min": std(price_5m_list),
                "price_mean_30min": mean(price_30m_list),
                "price_std_30min": std(price_30m_list),
                "size_tot_1min": total(size_1m_list),
                "size_tot_5min": total(size_5m_list),
                "size_tot_30min": total(size_30m_list),
                # SENTIMENT
                "sentiment_bluesky_mean_2h": mean(sentiment_bluesky_2h_list),
                "sentiment_bluesky_mean_1d": mean(sentiment_bluesky_1d_list),
                "sentiment_news_mean_1d": mean(sentiment_news_1d_list),
                "sentiment_news_mean_3d": mean(sentiment_news_3d_list),
                "sentiment_bluesky_mean_general_2hours": general_sentiment_dict["sentiment_bluesky_mean_general_2hours"],
                "sentiment_bluesky_mean_general_1d": general_sentiment_dict["sentiment_bluesky_mean_general_1d"],
                # NEW TIME-BASED FEATURES - unchanged
                "minutes_since_open": int(minutes_since_open),
                "day_of_week": int(now_ny.weekday()),
                "day_of_month": int(now_ny.day),
                "week_of_year": int(now_ny.isocalendar()[1]),
                "month_of_year": int(now_ny.month),
                "market_open_spike_flag": int(market_open_spike_flag),
                "market_close_spike_flag": int(market_close_spike_flag),
                # Fundamental data - unchanged
                "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
                "freeCashFlow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
                "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
                "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None,
                "is_simulated_prediction": is_simulated_prediction
            }

            for macro_key_alias, macro_value in macro_data_dict.items():
                features[macro_key_alias] = float(macro_value)

            result = json.dumps(features)
            print(f"[PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

            # Re-register the timer for the next 10-second interval - unchanged
            current_processing_time = ctx.timer_service().current_processing_time()
            next_timer_seconds = (current_processing_time // 1000 // 10 + 1) * 10
            if next_timer_seconds * 1000 == current_processing_time:
                next_timer_seconds += 10
            next_timer_timestamp = next_timer_seconds * 1000
            ctx.timer_service().register_processing_time_timer(next_timer_timestamp)
            self.last_timer_state.update(next_timer_timestamp)
            #print(f"[TIMER-RE-REGISTER] {ticker} - Next timer at {datetime.fromtimestamp(next_timer_timestamp / 1000, tz=timezone.utc).isoformat()}", file=sys.stderr)

            return [result]
        except Exception as e:
            print(f"[ERROR] on_timer for ticker {ticker}: {e}", file=sys.stderr)
            return [json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)})]

# --- Helper for splitting sentiment data ---
def expand_sentiment_data(json_str):
    """
    Expands a single sentiment JSON string into multiple if it contains a list of tickers,
    otherwise passes through other data types.
    Handles 'GENERAL' ticker by passing it through.
    """
    try:
        data = json.loads(json_str)
        
        if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
            expanded_records = []
            original_ticker_list = data["ticker"]
            
            if "GENERAL" in original_ticker_list:
                new_record = data.copy()
                new_record["ticker"] = "GENERAL"
                expanded_records.append(json.dumps(new_record))
                print(f"[WARN] Sentiment data with 'GENERAL' ticker detected and handled.", file=sys.stderr)
            
            for ticker_item in original_ticker_list:
                if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
                    new_record = data.copy()
                    new_record["ticker"] = ticker_item
                    expanded_records.append(json.dumps(new_record))
            
            if not expanded_records:
                print(f"[WARN] Sentiment data with no tracked or 'GENERAL' tickers: {json.loads(json_str).get('ticker')}", file=sys.stderr)
                return []
            return expanded_records
        
        return [json_str]
    except json.JSONDecodeError:
        print(f"[ERROR] Failed to decode JSON in expand_sentiment_data: {json_str}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[ERROR] expand_sentiment_data: {e} for {json_str}", file=sys.stderr)
        return []

def route_by_ticker(json_str):
    """Determines the key for incoming JSON data."""
    try:
        data = json.loads(json_str)
        if "alias" in data:
            return "macro_data_key"
        elif "ticker" in data:
            if data["ticker"] == "GENERAL":
                return GENERAL_SENTIMENT_KEY
            elif data["ticker"] in TOP_30_TICKERS:
                return data["ticker"]
            else:
                print(f"[WARN] Ticker '{data['ticker']}' not in TOP_30_TICKERS. Discarding.", file=sys.stderr)
                return "discard_key"
        else:
            print(f"[WARN] Data with no 'alias' or 'ticker' field: {json_str}", file=sys.stderr)
            return "unknown_data_key"
    except json.JSONDecodeError:
        print(f"[WARN] Failed to decode JSON for key_by: {json_str}", file=sys.stderr)
        return "invalid_json_key"
    except Exception as e:
        print(f"[ERROR] route_by_ticker: {e} for {json_str}", file=sys.stderr)
        return "error_key"


def main():
    load_fundamental_data()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # We keep parallelism at 1 for now to avoid issues
                            # with global dicts and data consistency.

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_stock_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics=["stock_trades", "macrodata", "news_sentiment", "bluesky_sentiment"],
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    producer = FlinkKafkaProducer(
        topic='aggregated_data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    
    expanded_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())

    keyed = expanded_stream.key_by(route_by_ticker, key_type=Types.STRING())
    
    processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
    
    processed.add_sink(producer)

    env.execute("Full Aggregation with Sliding Windows, Macrodata, Sentiment, Time, and Fundamental Features (Continuous)")

if __name__ == "__main__":
    main()