# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.datastream.state import ListStateDescriptor
# from datetime import datetime, timedelta
# import json
# import numpy as np
# import sys

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# class FullDayAggregator(KeyedProcessFunction):

#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)
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
#                 except Exception as e:
#                     print(f"[WARN] Skipping bad entry: {e}", file=sys.stderr)

#             parsed.sort(key=lambda x: x["timestamp"])

#             for i in range(len(parsed)):
#                 row = parsed[i]

#                 def window_vals(field, minutes):
#                     ts_limit = row["timestamp"] - timedelta(minutes=minutes)
#                     return [p[field] for p in parsed if ts_limit <= p["timestamp"] <= row["timestamp"]]

#                 def mean(values):
#                     return float(np.mean(values)) if values else None

#                 def std(values):
#                     return float(np.std(values)) if values else None

#                 def total(values):
#                     return float(np.sum(values)) if values else 0.0

#                 result = {
#                     "ticker": row["symbol"],
#                     "timestamp": row["timestamp"].isoformat(),
#                     "price_mean_1min": mean(window_vals("open", 1)),
#                     "price_mean_5min": mean(window_vals("open", 5)),
#                     "price_std_5min": std(window_vals("open", 5)),
#                     "price_mean_30min": mean(window_vals("open", 30)),
#                     "price_std_30min": std(window_vals("open", 30)),
#                     "size_tot_1min": total(window_vals("volume", 1)),
#                     "size_tot_5min": total(window_vals("volume", 5)),
#                     "size_tot_30min": total(window_vals("volume", 30)),
#                     "y1": row["close"]
#                 }
#                 result.update({
#                     "sentiment_bluesky_mean_2hours": 0.0,
#                     "sentiment_bluesky_mean_1day": 0.0,
#                     "sentiment_news_mean_1day": 0.0,
#                     "sentiment_news_mean_3days": 0.0,
#                     "sentiment_general_bluesky_mean_2hours": 0.0,
#                     "sentiment_general_bluesky_mean_1day": 0.0,
#                     "sentiment_reddit_mean_2hours": 0.0,
#                     "sentiment_reddit_mean_1day": 0.0,
#                     "sentiment_general_reddit_mean_2hours": 0.0,
#                     "sentiment_general_reddit_mean_1day": 0.0
#                 })
#                 if i <= len(parsed) - 5:
#                     result["y5"] = parsed[i + 4]["close"]

#                 print(json.dumps(result), flush=True)

#         except Exception as e:
#             print(f"[ERROR] Parsing message: {e}", file=sys.stderr)

# def extract_ticker_key(json_str):
#     try:
#         data = json.loads(json_str)
#         return data.get("ticker", "unknown")
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_batch_group',
#         'auto.offset.reset': 'earliest'
#     }

#     consumer = FlinkKafkaConsumer(
#         topics=["h_alpaca"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(extract_ticker_key, key_type=Types.STRING())
#     processed = keyed.process(FullDayAggregator())

#     env.execute("Historical Aggregation with Full Day Data")

# if __name__ == "__main__":
#     main()

































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from datetime import datetime, timedelta
# import json
# import numpy as np
# import sys

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# # Dati macroeconomici in memoria (series → list of (date, value))
# macro_data_by_series = {}

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


# class FullDayAggregator(KeyedProcessFunction):
#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)

#             # Caso MACRODATI
#             if "series" in message and "value" in message and "date" in message:
#                 try:
#                     series = message["series"]
#                     date = datetime.fromisoformat(message["date"]).date()
#                     value = float(message["value"])
#                     macro_data_by_series.setdefault(series, []).append((date, value))
#                     macro_data_by_series[series].sort()
#                 except Exception as e:
#                     print(f"[ERROR] Parsing macrodata: {e}", file=sys.stderr)
#                 return

#             # Caso ALPACA STORICI
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
#                 except Exception as e:
#                     print(f"[WARN] Skipping bad entry: {e}", file=sys.stderr)

#             parsed.sort(key=lambda x: x["timestamp"])

#             for i in range(len(parsed)):
#                 row = parsed[i]

#                 def window_vals(field, minutes):
#                     ts_limit = row["timestamp"] - timedelta(minutes=minutes)
#                     return [p[field] for p in parsed if ts_limit <= p["timestamp"] <= row["timestamp"]]

#                 def mean(values):
#                     return float(np.mean(values)) if values else None

#                 def std(values):
#                     return float(np.std(values)) if values else None

#                 def total(values):
#                     return float(np.sum(values)) if values else 0.0

#                 # Macro per la data corrente
#                 macro_values = get_macro_values_for_date(row["timestamp"].date())

#                 # Costruzione del record aggregato
#                 result = {
#                     "ticker": row["symbol"],
#                     "timestamp": row["timestamp"].isoformat(),
#                     "price_mean_1min": mean(window_vals("open", 1)),
#                     "price_mean_5min": mean(window_vals("open", 5)),
#                     "price_std_5min": std(window_vals("open", 5)),
#                     "price_mean_30min": mean(window_vals("open", 30)),
#                     "price_std_30min": std(window_vals("open", 30)),
#                     "size_tot_1min": total(window_vals("volume", 1)),
#                     "size_tot_5min": total(window_vals("volume", 5)),
#                     "size_tot_30min": total(window_vals("volume", 30)),
#                     # Sentiment placeholders
#                     "sentiment_bluesky_mean_2hours": 0.0,
#                     "sentiment_bluesky_mean_1day": 0.0,
#                     "sentiment_news_mean_1day": 0.0,
#                     "sentiment_news_mean_3days": 0.0,
#                     "sentiment_general_bluesky_mean_2hours": 0.0,
#                     "sentiment_general_bluesky_mean_1day": 0.0,
#                     "sentiment_reddit_mean_2hours": 0.0,
#                     "sentiment_reddit_mean_1day": 0.0,
#                     "sentiment_general_reddit_mean_2hours": 0.0,
#                     "sentiment_general_reddit_mean_1day": 0.0
#                 }

#                 # Aggiunta dei dati macro
#                 result.update(macro_values)

#                 # Aggiunta finale di target (y1 e y5)
#                 result["y1"] = row["close"]
#                 if i <= len(parsed) - 5:
#                     result["y5"] = parsed[i + 4]["close"]

#                 print(json.dumps(result), flush=True)

#         except Exception as e:
#             print(f"[ERROR] Processing element: {e}", file=sys.stderr)

# def extract_ticker_key(json_str):
#     try:
#         data = json.loads(json_str)
#         return data.get("ticker", data.get("series", "unknown"))
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_batch_group',
#         'auto.offset.reset': 'earliest'
#     }

#     consumer = FlinkKafkaConsumer(
#         topics=["h_alpaca", "h_macrodata"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(extract_ticker_key, key_type=Types.STRING())
#     processed = keyed.process(FullDayAggregator())

#     env.execute("Historical Aggregation with Macrodata and Sentiment")

# if __name__ == "__main__":
#     main()

































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from datetime import datetime, timedelta, time as dtime
# import json
# import numpy as np
# import sys
# from zoneinfo import ZoneInfo


# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# macro_data_by_series = {}
# fundamentals_by_symbol_year = {}

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
#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)

#             # MACRO DATA
#             if "series" in message and "value" in message and "date" in message:
#                 try:
#                     series = message["series"]
#                     date = datetime.fromisoformat(message["date"]).date()
#                     value = float(message["value"])
#                     macro_data_by_series.setdefault(series, []).append((date, value))
#                     macro_data_by_series[series].sort()
#                 except Exception as e:
#                     print(f"[ERROR] Parsing macrodata: {e}", file=sys.stderr)
#                 return

#             # FUNDAMENTAL COMPANY DATA
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
#                 except Exception as e:
#                     print(f"[ERROR] Parsing company data: {e}", file=sys.stderr)
#                 return

#             # HISTORICAL TICK DATA
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
#                 except Exception as e:
#                     print(f"[WARN] Skipping bad entry: {e}", file=sys.stderr)

#             parsed.sort(key=lambda x: x["timestamp"])

#             for i in range(len(parsed)):
#                 row = parsed[i]

#                 def window_vals(field, minutes):
#                     ts_limit = row["timestamp"] - timedelta(minutes=minutes)
#                     return [p[field] for p in parsed if ts_limit <= p["timestamp"] <= row["timestamp"]]

#                 def mean(values):
#                     return float(np.mean(values)) if values else None

#                 def std(values):
#                     return float(np.std(values)) if values else None

#                 def total(values):
#                     return float(np.sum(values)) if values else 0.0

#                 now = row["timestamp"]

#                 # Macro features
#                 macro_values = get_macro_values_for_date(now.date())

#                 # Fundamentals → usa calendarYear - 1
#                 fundamentals = get_fundamentals(row["symbol"], now.year - 1)

#                 # Derivate fondamentali
#                 profit_margin = None
#                 if fundamentals.get("revenue") not in [None, 0]:
#                     profit_margin = fundamentals["netIncome"] / fundamentals["revenue"]

#                 debt_to_equity = None
#                 if fundamentals.get("balance_totalStockholdersEquity") not in [None, 0]:
#                     debt_to_equity = fundamentals["balance_totalDebt"] / fundamentals["balance_totalStockholdersEquity"]

#                 local_time = row["timestamp"].astimezone(ZoneInfo("America/New_York"))
#                 market_open_flag = int(dtime(9, 30) <= local_time.time() <= dtime(9, 34))
#                 market_close_flag = int(dtime(15, 56) <= local_time.time() <= dtime(16, 00))

#                 # Feature finale
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

#                     # Sentiment placeholder
#                     "sentiment_bluesky_mean_2hours": 0.0,
#                     "sentiment_bluesky_mean_1day": 0.0,
#                     "sentiment_news_mean_1day": 0.0,
#                     "sentiment_news_mean_3days": 0.0,
#                     "sentiment_general_bluesky_mean_2hours": 0.0,
#                     "sentiment_general_bluesky_mean_1day": 0.0,
#                     # "sentiment_reddit_mean_2hours": 0.0,
#                     # "sentiment_reddit_mean_1day": 0.0,
#                     # "sentiment_general_reddit_mean_2hours": 0.0,
#                     # "sentiment_general_reddit_mean_1day": 0.0,

#                     # Time features
#                     "minutes_since_open": (now - now.replace(hour=13, minute=30, second=0, microsecond=0)).total_seconds() // 60,
#                     "day_of_week": now.weekday(),
#                     "day_of_month": now.day,
#                     "week_of_year": now.isocalendar()[1],
#                     "month_of_year": now.month,
#                     "market_open_spike_flag": market_open_flag,
#                     "market_close_spike_flag": market_close_flag,

#                     # Fundamental values
#                     "eps": fundamentals.get("eps"),
#                     "freeCashFlow": fundamentals.get("freeCashFlow"),
#                     "profit_margin": profit_margin,
#                     "debt_to_equity": debt_to_equity
#                 }

#                 result.update(macro_values)

#                 # Target
#                 result["y1"] = row["close"]
#                 if i <= len(parsed) - 5:
#                     result["y5"] = parsed[i + 4]["close"]

#                 print(json.dumps(result), flush=True)

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

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_batch_group',
#         'auto.offset.reset': 'earliest'
#     }

#     consumer = FlinkKafkaConsumer(
#         topics=["h_alpaca", "h_macrodata", "h_company"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(extract_key, key_type=Types.STRING())
#     processed = keyed.process(FullDayAggregator())

#     env.execute("Historical Aggregation with Macro + Fundamentals + Sentiment + Time Features")

# if __name__ == "__main__":
#     main()






















# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from datetime import datetime, timedelta, time as dtime
# from zoneinfo import ZoneInfo
# import json
# import numpy as np
# import sys

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# macro_data_by_series = {}
# fundamentals_by_symbol_year = {}
# pending_stock_batches = []
# ready_flags = {
#     "macro_ready": False,
#     "fundamentals_ready": False
# }

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
#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)
#             print(f"[DEBUG] Received message keys: {list(message.keys())}", file=sys.stderr)

#             # Handle macro data
#             if "series" in message and "value" in message and "date" in message:
#                 try:
#                     print(f"[INFO] MACRO DATA RECEIVED ✅", file=sys.stderr)
#                     series = message["series"]
#                     date = datetime.fromisoformat(message["date"]).date()
#                     value = float(message["value"])
#                     macro_data_by_series.setdefault(series, []).append((date, value))
#                     macro_data_by_series[series].sort()
#                     ready_flags["macro_ready"] = True
#                 except Exception as e:
#                     print(f"[ERROR] Parsing macrodata: {e}", file=sys.stderr)
#                 return

#             # Handle fundamental data
#             if "symbol" in message and "calendarYear" in message:
#                 try:
#                     print(f"[INFO] FUNDAMENTALS DATA RECEIVED ✅", file=sys.stderr)
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
#                     print(f"[ERROR] Parsing company data: {e}", file=sys.stderr)
#                 return

#             # Process buffered stock data if ready
#             if ready_flags["macro_ready"] and ready_flags["fundamentals_ready"] and pending_stock_batches:
#                 print(f"[INFO] Releasing {len(pending_stock_batches)} buffered batches", file=sys.stderr)
#                 for buffered_value in pending_stock_batches:
#                     yield from self.process_element(buffered_value, ctx)
#                 pending_stock_batches.clear()

#             # Wait until macro and fundamental data have arrived
#             if not (ready_flags["macro_ready"] and ready_flags["fundamentals_ready"]):
#                 print("[INFO] Buffering stock data, waiting for macro/fundamentals...", file=sys.stderr)
#                 pending_stock_batches.append(value)
#                 return

#             # Process historical alpaca data
#             ticker = message.get("ticker")
#             if ticker not in TOP_30_TICKERS:
#                 print(f"[INFO] Ignoring ticker {ticker}", file=sys.stderr)
#                 return

#             rows = message.get("data", [])
#             print(f"[INFO] Processing stock for {ticker}, received {len(rows)} rows", file=sys.stderr)

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
#                 except Exception as e:
#                     print(f"[WARN] Skipping bad entry: {e}", file=sys.stderr)

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

#                     # Sentiment placeholder
#                     "sentiment_bluesky_mean_2hours": 0.0,
#                     "sentiment_bluesky_mean_1day": 0.0,
#                     "sentiment_news_mean_1day": 0.0,
#                     "sentiment_news_mean_3days": 0.0,
#                     "sentiment_general_bluesky_mean_2hours": 0.0,
#                     "sentiment_general_bluesky_mean_1day": 0.0,

#                     # Time features
#                     "minutes_since_open": (now - now.replace(hour=13, minute=30)).total_seconds() // 60,
#                     "day_of_week": now.weekday(),
#                     "day_of_month": now.day,
#                     "week_of_year": now.isocalendar()[1],
#                     "month_of_year": now.month,
#                     "market_open_spike_flag": market_open_flag,
#                     "market_close_spike_flag": market_close_flag,

#                     # Fundamentals
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

#             if batch_results:
#                 print(f"[INFO] Yielding batch with {len(batch_results)} rows for {ticker}", file=sys.stderr)
#                 yield json.dumps(batch_results)

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
#             "bootstrap.servers": "kafka:9092",
#             "group.id": "flink_batch_group",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     producer = FlinkKafkaProducer(
#         topic="h_aggregated",
#         serialization_schema=SimpleStringSchema(),
#         producer_config={"bootstrap.servers": "kafka:9092"}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(extract_key, key_type=Types.STRING())
#     processed = keyed.process(FullDayAggregator(), output_type=Types.STRING())
#     processed.add_sink(producer)

#     env.execute("Historical Aggregation to Kafka - Batch Mode")

# if __name__ == "__main__":
#     main()



# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from datetime import datetime, timedelta, time as dtime
# from zoneinfo import ZoneInfo
# import json
# import numpy as np
# import sys

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# macro_data_by_series = {}
# fundamentals_by_symbol_year = {}
# pending_stock_batches = []
# ready_flags = {
#     "macro_ready": False,
#     "fundamentals_ready": False
# }

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
#     def process_element(self, value, ctx):
#         try:
#             message = json.loads(value)

#             if "series" in message and "value" in message and "date" in message:
#                 try:
#                     series = message["series"]
#                     date = datetime.fromisoformat(message["date"]).date()
#                     value = float(message["value"])
#                     macro_data_by_series.setdefault(series, []).append((date, value))
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

#             if batch_results:
#                 print(
#                     json.dumps(batch_results[:3], indent=2),
#                     file=sys.stderr
#                 )
#                 yield json.dumps(batch_results)

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
#             "bootstrap.servers": "kafka:9092",
#             "group.id": "flink_batch_group",
#             "auto.offset.reset": "earliest"
#         }
#     )

#     producer = FlinkKafkaProducer(
#         topic="h_aggregated",
#         serialization_schema=SimpleStringSchema(),
#         producer_config={"bootstrap.servers": "kafka:9092"}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(extract_key, key_type=Types.STRING())
#     processed = keyed.process(FullDayAggregator(), output_type=Types.STRING())
#     processed.add_sink(producer)

#     env.execute("Historical Aggregation to Kafka - Batch Mode")

# if __name__ == "__main__":
#     main()


from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from minio import Minio
from io import BytesIO
import json
import numpy as np
import pandas as pd
import os
import sys

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
        self.bucket_name = "aggregated-data"
        self.minio_client = Minio(
            endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "admin123"),
            secure=False
        )
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)

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
                    ready_flags["macro_ready"] = True
                except:
                    pass
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
                except:
                    pass
                return

            if ready_flags["macro_ready"] and ready_flags["fundamentals_ready"] and pending_stock_batches:
                for buffered_value in pending_stock_batches:
                    yield from self.process_element(buffered_value, ctx)
                pending_stock_batches.clear()

            if not (ready_flags["macro_ready"] and ready_flags["fundamentals_ready"]):
                pending_stock_batches.append(value)
                return

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
                except:
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
                    "minutes_since_open": (now - now.replace(hour=13, minute=30)).total_seconds() // 60,
                    "day_of_week": now.weekday(),
                    "day_of_month": now.day,
                    "week_of_year": now.isocalendar()[1],
                    "month_of_year": now.month,
                    "market_open_spike_flag": market_open_flag,
                    "market_close_spike_flag": market_close_flag,
                    "eps": fundamentals.get("eps"),
                    "freeCashFlow": fundamentals.get("freeCashFlow"),
                    "profit_margin": profit_margin,
                    "debt_to_equity": debt_to_equity
                }

                result.update(macro_values)
                result["y1"] = row["close"]
                if i <= len(parsed) - 5:
                    result["y5"] = parsed[i + 4]["close"]

                batch_results.append(result)

            # Scrittura su MinIO
            if batch_results:
                df = pd.DataFrame(batch_results)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df["date"] = df["timestamp"].dt.date

                for (symbol, day), group in df.groupby(["ticker", "date"]):
                    year = day.year
                    month = f"{day.month:02d}"
                    day_str = f"{day.day:02d}"
                    parquet_buffer = BytesIO()
                    group.drop(columns="date").to_parquet(parquet_buffer, index=False)
                    parquet_buffer.seek(0)

                    object_name = f"{symbol}/{year}/{month}/{day_str}.parquet"
                    self.minio_client.put_object(
                        self.bucket_name,
                        object_name,
                        parquet_buffer,
                        length=parquet_buffer.getbuffer().nbytes,
                        content_type="application/octet-stream"
                    )

        except Exception as e:
            print(f"[ERROR] Processing element: {e}", file=sys.stderr)

def extract_key(json_str):
    try:
        data = json.loads(json_str)
        return data.get("ticker") or data.get("symbol") or data.get("series") or "unknown"
    except:
        return "unknown"

def main():
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
    processed = keyed.process(FullDayAggregator(), output_type=Types.STRING())

    env.execute("Historical Aggregation to MinIO")

if __name__ == "__main__":
    main()
