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























from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor
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

# Flink State Descriptors
fundamentals_state_descriptor = MapStateDescriptor(
    "fundamentals_data_state",
    Types.INT(), # Key: year
    Types.MAP(Types.STRING(), Types.GENERIC_ARRAY()) # Value: dict of fundamental data
)

pending_stock_batches_descriptor = MapStateDescriptor(
    "pending_stock_batches_state",
    Types.LONG(),    # Key: timestamp (or unique message ID)
    Types.STRING()   # Value: original JSON message string
)

class FullDayAggregator(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.fundamentals_data_state = self.get_runtime_context().get_map_state(fundamentals_state_descriptor)
        self.pending_stock_batches = self.get_runtime_context().get_map_state(pending_stock_batches_descriptor)

        # These flags are local to each TaskManager and not fault-tolerant.
        self._local_macro_data_by_series = {}
        self._macro_ready_local = False
        self._fundamentals_ready_local = False # This flag is local to the TaskManager

        # Initialize PostgreSQL connection
        self.pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgre"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "aggregated-data"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "admin123")
        )
        self.pg_cursor = self.pg_conn.cursor()

        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS aggregated_stock_data (
            id SERIAL PRIMARY KEY,
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
            freeCashFlow DOUBLE PRECISION,
            profit_margin DOUBLE PRECISION,
            debt_to_equity DOUBLE PRECISION,
            fed_funds_rate DOUBLE PRECISION,
            consumer_price_index DOUBLE PRECISION,
            gdp DOUBLE PRECISION,
            y1 DOUBLE PRECISION,
            y5 DOUBLE PRECISION
        );
        """
        self.pg_cursor.execute(create_table_query)
        self.pg_conn.commit()
        print("Table 'aggregated_stock_data' verified/created in PostgreSQL.")

    def close(self):
        # Close PostgreSQL connection
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
        print("PostgreSQL connection closed.")

    def process_element(self, value, ctx):
        try:
            message = json.loads(value)
            current_key = ctx.get_current_key()

            # Handle Macro Data (local to TaskManager, not fault-tolerant)
            if "series" in message and "value" in message and "date" in message:
                try:
                    series = message["series"]
                    date_val = datetime.fromisoformat(message["date"]).date()
                    val = float(message["value"])
                    self._local_macro_data_by_series.setdefault(series, []).append((date_val, val))
                    self._local_macro_data_by_series[series].sort(key=lambda x: x[0])
                    if len(self._local_macro_data_by_series) > 0:
                        self._macro_ready_local = True
                except (ValueError, KeyError) as e:
                    print(f"[WARN] Failed to parse macro data: {e} - {message}", file=sys.stderr)
                return

            # Handle Fundamental Data (Flink State, keyed by symbol)
            if "symbol" in message and "calendarYear" in message:
                try:
                    year = int(message["calendarYear"])
                    fundamentals_for_symbol = {
                        "eps": message.get("eps"),
                        "freeCashFlow": message.get("cashflow_freeCashFlow"),
                        "revenue": message.get("revenue"),
                        "netIncome": message.get("netIncome"),
                        "balance_totalDebt": message.get("balance_totalDebt"),
                        "balance_totalStockholdersEquity": message.get("balance_totalStockholdersEquity")
                    }
                    self.fundamentals_data_state.put(year, fundamentals_for_symbol)
                    self._fundamentals_ready_local = True
                except (ValueError, KeyError) as e:
                    print(f"[WARN] Failed to parse fundamental data: {e} - {message}", file=sys.stderr)
                return

            # Process pending batches and buffer stock data
            if self._macro_ready_local and self._fundamentals_ready_local:
                current_pending_batches = list(self.pending_stock_batches.values())
                if current_pending_batches:
                    print(f"[{datetime.now()}] Ready! Processing {len(current_pending_batches)} pending batches for key {current_key}...")
                    for buffered_value_str in current_pending_batches:
                        self._process_stock_data(buffered_value_str, ctx) # Call helper directly
                    self.pending_stock_batches.clear()

            if not (self._macro_ready_local and self._fundamentals_ready_local):
                ticker = message.get("ticker")
                if ticker and ticker in TOP_30_TICKERS:
                    self.pending_stock_batches.put(int(datetime.now().timestamp() * 1000), value)
                return

            # Process real-time stock data
            self._process_stock_data(value, ctx)

        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON decoding failed: {e} for value: {value}", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] General processing error: {e}", file=sys.stderr)

    def _process_stock_data(self, value_str, ctx):
        """Helper method to process stock data messages."""
        message = json.loads(value_str)

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
            except (ValueError, KeyError) as e:
                print(f"[WARN] Failed to parse stock entry: {e} - {entry}", file=sys.stderr)
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

            # Access local macro data
            macro_values = {}
            for series, entries in self._local_macro_data_by_series.items():
                selected_value = None
                for entry_date, value in entries:
                    if entry_date <= now.date():
                        selected_value = value
                    else:
                        break
                if selected_value is not None:
                    macro_values[series] = selected_value

            # Access fundamental data from Flink state
            fundamentals = self.fundamentals_data_state.get(now.year - 1)
            fundamentals = fundamentals if fundamentals is not None else {}

            profit_margin = None
            if fundamentals.get("revenue") not in [None, 0] and fundamentals.get("netIncome") is not None:
                profit_margin = fundamentals["netIncome"] / fundamentals["revenue"]

            debt_to_equity = None
            if fundamentals.get("balance_totalStockholdersEquity") not in [None, 0] and fundamentals.get("balance_totalDebt") is not None:
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
                "minutes_since_open": (now - now.replace(hour=13, minute=30, second=0, microsecond=0)).total_seconds() // 60,
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

            macro_column_mapping = {
                "FED.FUNDS": "fed_funds_rate",
                "CPI": "consumer_price_index",
                "GDP": "gdp"
            }
            for series_name, value_macro in macro_values.items():
                sql_col_name = macro_column_mapping.get(series_name)
                if sql_col_name:
                    result[sql_col_name] = value_macro

            result["y1"] = row["close"]
            if i <= len(parsed) - 5:
                result["y5"] = parsed[i + 4]["close"]
            else:
                result["y5"] = None

            batch_results.append(result)

        # Write to PostgreSQL
        if batch_results:
            self.pg_cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'aggregated_stock_data' ORDER BY ordinal_position;")
            db_columns = [row[0] for row in self.pg_cursor.fetchall()]
            
            insert_columns = [col for col in db_columns if col != 'id']
            
            placeholders = ', '.join(['%s'] * len(insert_columns))
            column_names_sql = ', '.join([sql.Identifier(col).as_string(self.pg_conn) for col in insert_columns])
            
            insert_query = sql.SQL(
                "INSERT INTO aggregated_stock_data ({}) VALUES ({})"
            ).format(sql.SQL(column_names_sql), sql.SQL(placeholders))

            records_to_insert = []
            for record in batch_results:
                record["timestamp"] = datetime.fromisoformat(record["timestamp"])
                
                row_values = []
                for col in insert_columns:
                    row_values.append(record.get(col)) 
                records_to_insert.append(tuple(row_values))

            try:
                self.pg_cursor.executemany(insert_query, records_to_insert)
                self.pg_conn.commit()
                print(f"[{datetime.now()}] Inserted {len(records_to_insert)} rows into PostgreSQL for {ticker}.")
            except Exception as e:
                self.pg_conn.rollback()
                print(f"[ERROR] Failed to insert into PostgreSQL: {e}", file=sys.stderr)

def extract_key(json_str):
    try:
        data = json.loads(json_str)
        if "series" in data:
            return "GLOBAL_MACRO_KEY" # All macro data goes to a fixed key
        elif "symbol" in data:
            return data["symbol"]
        elif "ticker" in data:
            return data["ticker"]
        return "unknown"
    except:
        return "unknown"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Set parallelism to 1 for local development.
    # If deploying to a Flink cluster, this line can be removed or set to a higher value
    # to leverage cluster resources.
    env.set_parallelism(1) 

    consumer = FlinkKafkaConsumer(
        topics=["h_alpaca", "h_macrodata", "h_company"],
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": "flink_postgres_aggregator_v2",
            "auto.offset.reset": "earliest"
        }
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    
    keyed = stream.key_by(extract_key, key_type=Types.STRING())
    processed = keyed.process(FullDayAggregator(), output_type=Types.STRING())

    env.execute("Historical Aggregation to PostgreSQL (Distributed)")

if __name__ == "__main__":
    main()