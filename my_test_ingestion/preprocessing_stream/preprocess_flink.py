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

# Global dictionaries for shared state (read-only after load)
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
    print("🚀 [INIT] Loading fundamental data from MinIO...", file=sys.stderr)
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
                    print(f"✅ [FUNDAMENTALS] Loaded data for {ticker}: {fundamentals_data[ticker]}", file=sys.stderr)
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
        print("✅ [INIT] Fundamental data loading complete.", file=sys.stderr)

    except Exception as e:
        print(f"[CRITICAL] Failed to initialize Minio client or load any fundamental data: {e}", file=sys.stderr)

class SlidingAggregator(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        def descriptor(name):
            return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

        self.price_1m = runtime_context.get_map_state(descriptor("price_1m"))
        self.price_5m = runtime_context.get_map_state(descriptor("price_5m"))
        self.price_30m = runtime_context.get_map_state(descriptor("price_30m"))

        self.size_1m = runtime_context.get_map_state(descriptor("size_1m"))
        self.size_5m = runtime_context.get_map_state(descriptor("size_5m"))
        self.size_30m = runtime_context.get_map_state(descriptor("size_30m"))

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
                    # Convert to float to ensure JSON serializability later
                    macro_data_dict[alias_key] = float(data["value"])
                    print(f"📥 [MACRO] {alias_key}: {macro_data_dict[alias_key]}", file=sys.stderr)
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
                        print(f"📥 [SENTIMENT-GENERAL] Bluesky - {ts_str}: {sentiment_score}", file=sys.stderr)
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
                    print(f"📥 [SENTIMENT] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr)
                return []

            # --- Handle Stock Trade Data ---
            elif "price" in data and "size" in data:
                ticker = data.get("ticker")
                if ticker not in TOP_30_TICKERS:
                    return []
                
                ts_str = data.get("timestamp")
                if not ts_str:
                    print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
                    return []

                event_time_utc = isoparse(ts_str).replace(tzinfo=timezone.utc)
                event_time_ny = event_time_utc.astimezone(NY_TZ)

                market_open_data_reception = event_time_ny.replace(hour=9, minute=30, second=0, microsecond=0)
                market_close_data_reception = event_time_ny.replace(hour=16, minute=0, second=0, microsecond=0)

                if not (market_open_data_reception <= event_time_ny < market_close_data_reception):
                    print(f"[DEBUG] Skipping trade data for {ticker} outside market reception hours ({event_time_ny.strftime('%H:%M')} NYT).", file=sys.stderr)
                    return []

                price = float(data.get("price"))
                size = float(data.get("size"))

                for state in [self.price_1m, self.price_5m, self.price_30m]:
                    state.put(ts_str, price)
                for state in [self.size_1m, self.size_5m, self.size_30m]:
                    state.put(ts_str, size)
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
            last_timer = self.last_timer_state.value()
            if last_timer is None or ctx.timer_service().current_processing_time() >= last_timer:
                next_ts = ctx.timer_service().current_processing_time() + 5000
                ctx.timer_service().register_processing_time_timer(next_ts)
                self.last_timer_state.update(next_ts)


    def on_timer(self, timestamp, ctx):
        """Called when a registered timer fires."""
        try:
            now_utc = datetime.now(timezone.utc)
            ts_str = now_utc.isoformat()
            ticker = ctx.get_current_key()

            def mean(vals):
                vals = list(vals)
                return float(np.mean(vals)) if vals else 0.0

            def std(vals):
                vals = list(vals)
                return float(np.std(vals)) if vals and len(vals) > 1 else 0.0

            def total(vals):
                vals = list(vals)
                return float(np.sum(vals)) if vals else 0.0

            # --- Handle GENERAL Sentiment Key ---
            if ticker == GENERAL_SENTIMENT_KEY:
                self._cleanup_old_entries(self.sentiment_bluesky_general_2h, 2 * 60)
                self._cleanup_old_entries(self.sentiment_bluesky_general_1d, 24 * 60)

                general_sentiment_dict["sentiment_bluesky_mean_general_2hours"] = mean(self.sentiment_bluesky_general_2h.values())
                general_sentiment_dict["sentiment_bluesky_mean_general_1d"] = mean(self.sentiment_bluesky_general_1d.values())
                
                print(f"🔄 [GENERAL SENTIMENT AGG] Updated general_sentiment_dict: {general_sentiment_dict}", file=sys.stderr)
                return []

            # --- Handle Macro Data Key ---
            if ticker == "macro_data_key":
                return []

            # --- Handle Specific Ticker Data (TOP_30_TICKERS) ---
            if ticker not in TOP_30_TICKERS:
                print(f"[WARN] on_timer fired for unexpected key: {ticker}", file=sys.stderr)
                return []

            now_ny = now_utc.astimezone(NY_TZ)

            market_open_prediction = now_ny.replace(hour=9, minute=31, second=0, microsecond=0)
            market_close_prediction = now_ny.replace(hour=15, minute=59, second=0, microsecond=0)

            if not (market_open_prediction <= now_ny <= market_close_prediction):
                print(f"[DEBUG] Skipping prediction for {ticker} outside market prediction hours ({now_ny.strftime('%H:%M')} NYT).", file=sys.stderr)
                return []

            self._cleanup_old_entries(self.price_1m, 1)
            self._cleanup_old_entries(self.price_5m, 5)
            self._cleanup_old_entries(self.price_30m, 30)
            self._cleanup_old_entries(self.size_1m, 1)
            self._cleanup_old_entries(self.size_5m, 5)
            self._cleanup_old_entries(self.size_30m, 30)
            self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
            self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
            self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
            self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)

            market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)

            market_open_spike_flag = 0
            market_close_spike_flag = 0

            if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
                market_open_spike_flag = 1
            
            if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
                market_close_spike_flag = 1

            ticker_fundamentals = fundamentals_data.get(ticker, {})

            features = {
                "ticker": ticker,
                "timestamp": ts_str,
                "price_mean_1min": mean(self.price_1m.values()),
                "price_mean_5min": mean(self.price_5m.values()),
                "price_std_5min": std(self.price_5m.values()),
                "price_mean_30min": mean(self.price_30m.values()),
                "price_std_30min": std(self.price_30m.values()),
                "size_tot_1min": total(self.size_1m.values()),
                "size_tot_5min": total(self.size_5m.values()),
                "size_tot_30min": total(self.size_30m.values()),
                #SENTIMENT
                "sentiment_bluesky_mean_2h": mean(self.sentiment_bluesky_2h.values()),
                "sentiment_bluesky_mean_1d": mean(self.sentiment_bluesky_1d.values()),
                "sentiment_news_mean_1d": mean(self.sentiment_news_1d.values()),
                "sentiment_news_mean_3d": mean(self.sentiment_news_3d.values()),
                "sentiment_bluesky_mean_general_2hours": general_sentiment_dict["sentiment_bluesky_mean_general_2hours"],
                "sentiment_bluesky_mean_general_1d": general_sentiment_dict["sentiment_bluesky_mean_general_1d"],
                # NEW TIME-BASED FEATURES - Ensure these are Python native int/float
                "minutes_since_open": int((now_ny - market_open_time).total_seconds() // 60) if now_ny >= market_open_time else -1,
                "day_of_week": int(now_ny.weekday()),
                "day_of_month": int(now_ny.day),
                "week_of_year": int(now_ny.isocalendar()[1]),
                "month_of_year": int(now_ny.month),
                "market_open_spike_flag": int(market_open_spike_flag),
                "market_close_spike_flag": int(market_close_spike_flag),
                # Fundamental data - ensure these are Python native floats
                "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
                "freeCashFlow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
                "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
                "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None
            }

            for macro_key_alias, macro_value in macro_data_dict.items():
                # Ensure macro values are converted to float
                features[macro_key_alias] = float(macro_value)

            result = json.dumps(features)
            print(f"📤 [PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

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
                print(f"[WARN] Sentiment data with no tracked or 'GENERAL' tickers: {json_str}", file=sys.stderr)
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
    env.set_parallelism(1)

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
        topic='aggregated-data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    
    expanded_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())

    keyed = expanded_stream.key_by(route_by_ticker, key_type=Types.STRING())
    
    processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
    
    processed.add_sink(producer)

    env.execute("Full Aggregation with Sliding Windows, Macrodata, Sentiment, Time, and Fundamental Features")

if __name__ == "__main__":
    main()




































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from datetime import datetime, timezone, timedelta
# import json
# import numpy as np
# import sys

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

# macro_data_dict = {}

# class SlidingAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         self.price_1m = runtime_context.get_map_state(descriptor("price_1m"))
#         self.price_5m = runtime_context.get_map_state(descriptor("price_5m"))
#         self.price_30m = runtime_context.get_map_state(descriptor("price_30m"))

#         self.size_1m = runtime_context.get_map_state(descriptor("size_1m"))
#         self.size_5m = runtime_context.get_map_state(descriptor("size_5m"))
#         self.size_30m = runtime_context.get_map_state(descriptor("size_30m"))

#         self.last_timer_state = runtime_context.get_state(
#             ValueStateDescriptor("last_timer", Types.LONG()))

#     def _cleanup_old_entries(self, state, window_minutes):
#         threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
#         for k in list(state.keys()):
#             try:
#                 if datetime.fromisoformat(k) < threshold:
#                     state.remove(k)
#             except:
#                 state.remove(k)

#     def process_element(self, value, ctx):
#         try:
#             data = json.loads(value)
#             if "alias" in data and "value" in data:
#                 alias_key = data.get("alias")
#                 if alias_key:
#                     macro_data_dict[alias_key] = float(data["value"])
#                     print(macro_data_dict)
#                 return

#             ticker = data.get("ticker")
#             if ticker not in TOP_30_TICKERS:
#                 return

#             ctx.set_current_key(ticker)

#             ts_str = data.get("timestamp")
#             price = float(data.get("price"))
#             size = float(data.get("size"))

#             for state in [self.price_1m, self.price_5m, self.price_30m]:
#                 state.put(ts_str, price)
#             for state in [self.size_1m, self.size_5m, self.size_30m]:
#                 state.put(ts_str, size)

#             self._cleanup_old_entries(self.price_1m, 1)
#             self._cleanup_old_entries(self.price_5m, 5)
#             self._cleanup_old_entries(self.price_30m, 30)
#             self._cleanup_old_entries(self.size_1m, 1)
#             self._cleanup_old_entries(self.size_5m, 5)
#             self._cleanup_old_entries(self.size_30m, 30)

#             last_timer = self.last_timer_state.value()
#             if last_timer is None:
#                 next_ts = ctx.timer_service().current_processing_time() + 60000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)

#         except Exception as e:
#             print(f"[ERROR] process_element: {e}", file=sys.stderr)

#     def on_timer(self, timestamp, ctx):
#         try:
#             now = datetime.now(timezone.utc)
#             ts_str = now.isoformat()
#             ticker = ctx.get_current_key()

#             def mean(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else None

#             def std(vals):
#                 vals = list(vals)
#                 return float(np.std(vals)) if vals else None

#             def total(vals):
#                 vals = list(vals)
#                 return float(np.sum(vals)) if vals else 0.0

#             features = {
#                 "ticker": ticker,
#                 "timestamp": ts_str,
#                 "price_mean_1min": mean(self.price_1m.values()),
#                 "price_mean_5min": mean(self.price_5m.values()),
#                 "price_std_5min": std(self.price_5m.values()),
#                 "price_mean_30min": mean(self.price_30m.values()),
#                 "price_std_30min": std(self.price_30m.values()),
#                 "size_tot_1min": total(self.size_1m.values()),
#                 "size_tot_5min": total(self.size_5m.values()),
#                 "size_tot_30min": total(self.size_30m.values()),
#             }

#             for macro_key in macro_alias.values():
#                 features[macro_key] = macro_data_dict.get(macro_key)

#             result = json.dumps(features)
#             print(f"📤 [SENT2] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             next_ts = ctx.timer_service().current_processing_time() + 5000
#             ctx.timer_service().register_processing_time_timer(next_ts)
#             self.last_timer_state.update(next_ts)

#             return result
#         except Exception as e:
#             print(f"[ERROR] on_timer: {e}", file=sys.stderr)
#             return json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat()})

# def route_by_ticker(json_str):
#     try:
#         data = json.loads(json_str)
#         return data.get("ticker") or "unknown"
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_group',
#         'auto.offset.reset': 'earliest'
#     }

#     consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "macrodata"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregated-data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(route_by_ticker, key_type=Types.STRING())
#     processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
#     processed.add_sink(producer)

#     env.execute("Full Aggregation with Sliding Windows and Macrodata")

# if __name__ == "__main__":
#     main()

























































































































































































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from datetime import datetime, timezone, timedelta
# import json
# import numpy as np
# import sys

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
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

# macro_data_dict = {}

# class SlidingAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         self.price_1m = runtime_context.get_map_state(descriptor("price_1m"))
#         self.price_5m = runtime_context.get_map_state(descriptor("price_5m"))
#         self.price_30m = runtime_context.get_map_state(descriptor("price_30m"))

#         self.size_1m = runtime_context.get_map_state(descriptor("size_1m"))
#         self.size_5m = runtime_context.get_map_state(descriptor("size_5m"))
#         self.size_30m = runtime_context.get_map_state(descriptor("size_30m"))

#         self.sentiment_bluesky_state_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_state_2h"))
#         self.sentiment_bluesky_state_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_state_1d"))
#         # self.sentiment_reddit_state_2h = runtime_context.get_map_state(descriptor("sentiment_reddit_state_2h"))
#         # self.sentiment_reddit_state_1d = runtime_context.get_map_state(descriptor("sentiment_reddit_state_1d"))
#         self.sentiment_news_state_1d = runtime_context.get_map_state(descriptor("sentiment_news_state_1d"))
#         self.sentiment_news_state_3d = runtime_context.get_map_state(descriptor("sentiment_news_state_3d"))

#         self.last_timer_state = runtime_context.get_state(
#             ValueStateDescriptor("last_timer", Types.LONG()))

#     def _cleanup_old_entries(self, state, window_minutes):
#         threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
#         for k in list(state.keys()):
#             try:
#                 if datetime.fromisoformat(k) < threshold:
#                     state.remove(k)
#             except:
#                 state.remove(k)

#     def process_element(self, value, ctx):
#         try:
#             data = json.loads(value)
#             now = datetime.now(timezone.utc)
#             ts_str = data.get("timestamp", now.isoformat())
#             ticker = data.get("ticker")

#             if "alias" in data and "value" in data:
#                 alias_key = data.get("alias")
#                 if alias_key:
#                     macro_data_dict[alias_key] = float(data["value"])
#                 return

#             if "sentiment" in data and ticker:
#                 source = data.get("source")
#                 sentiment = float(data.get("sentiment"))
#                 if source == "bluesky":
#                     self.sentiment_bluesky_state_2h.put(ts_str, sentiment)
#                     self.sentiment_bluesky_state_1d.put(ts_str, sentiment)
#                     self._cleanup_old_entries(self.sentiment_bluesky_state_2h, 120)
#                     self._cleanup_old_entries(self.sentiment_bluesky_state_1d, 1440)
#                 elif source == "news":
#                     self.sentiment_news_state_1d.put(ts_str, sentiment)
#                     self.sentiment_news_state_3d.put(ts_str, sentiment)
#                     self._cleanup_old_entries(self.sentiment_news_state_1d, 1440)
#                     self._cleanup_old_entries(self.sentiment_news_state_3d, 4320)
#                 return

#             if ticker not in TOP_30_TICKERS:
#                 return

#             ctx.set_current_key(ticker)
#             price = float(data.get("price"))
#             size = float(data.get("size"))

#             for state in [self.price_1m, self.price_5m, self.price_30m]:
#                 state.put(ts_str, price)
#             for state in [self.size_1m, self.size_5m, self.size_30m]:
#                 state.put(ts_str, size)

#             self._cleanup_old_entries(self.price_1m, 1)
#             self._cleanup_old_entries(self.price_5m, 5)
#             self._cleanup_old_entries(self.price_30m, 30)
#             self._cleanup_old_entries(self.size_1m, 1)
#             self._cleanup_old_entries(self.size_5m, 5)
#             self._cleanup_old_entries(self.size_30m, 30)

#             last_timer = self.last_timer_state.value()
#             if last_timer is None:
#                 next_ts = ctx.timer_service().current_processing_time() + 60000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)

#         except Exception as e:
#             print(f"[ERROR] process_element: {e}", file=sys.stderr)

#     def on_timer(self, timestamp, ctx):
#         try:
#             now = datetime.now(timezone.utc)
#             ts_str = now.isoformat()
#             ticker = ctx.get_current_key()

#             def mean(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else None

#             def std(vals):
#                 vals = list(vals)
#                 return float(np.std(vals)) if vals else None

#             def total(vals):
#                 vals = list(vals)
#                 return float(np.sum(vals)) if vals else 0.0

#             def mean_sent(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else 0.0

#             features = {
#                 "ticker": ticker,
#                 "timestamp": ts_str,
#                 "price_mean_1min": mean(self.price_1m.values()),
#                 "price_mean_5min": mean(self.price_5m.values()),
#                 "price_std_5min": std(self.price_5m.values()),
#                 "price_mean_30min": mean(self.price_30m.values()),
#                 "price_std_30min": std(self.price_30m.values()),
#                 "size_tot_1min": total(self.size_1m.values()),
#                 "size_tot_5min": total(self.size_5m.values()),
#                 "size_tot_30min": total(self.size_30m.values()),
#                 "sentiment_bluesky_mean_2hours": mean_sent(self.sentiment_bluesky_state_2h.values()),
#                 "sentiment_bluesky_mean_1day": mean_sent(self.sentiment_bluesky_state_1d.values()),
#                 # "sentiment_reddit_mean_2hours": mean_sent(self.sentiment_reddit_state_2h.values()),
#                 # "sentiment_reddit_mean_1day": mean_sent(self.sentiment_reddit_state_1d.values()),
#                 "sentiment_news_mean_1day": mean_sent(self.sentiment_news_state_1d.values()),
#                 "sentiment_news_mean_3days": mean_sent(self.sentiment_news_state_3d.values()),
#             }

#             for macro_key in macro_alias.values():
#                 features[macro_key] = macro_data_dict.get(macro_key)

#             result = json.dumps(features)
#             print(f"📤 [SENT2] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             next_ts = ctx.timer_service().current_processing_time() + 5000
#             ctx.timer_service().register_processing_time_timer(next_ts)
#             self.last_timer_state.update(next_ts)

#             return result
#         except Exception as e:
#             print(f"[ERROR] on_timer: {e}", file=sys.stderr)
#             return json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat()})

# def route_by_ticker(json_str):
#     try:
#         data = json.loads(json_str)
#         return data.get("ticker") or "unknown"
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_group',
#         'auto.offset.reset': 'earliest'
#     }

#     consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "macrodata", "bluesky_sentiment", "news_sentiment"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregated-data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(route_by_ticker, key_type=Types.STRING())
#     processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
#     processed.add_sink(producer)

#     env.execute("Full Aggregation with Sliding Windows and Macrodata")

# if __name__ == "__main__":
#     main()



























































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from pyflink.table import EnvironmentSettings, TableEnvironment
# from datetime import datetime, timezone, timedelta
# import json
# import numpy as np
# import sys

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

# macro_data_dict = {}
# fundamentals_data = {}

# def load_fundamentals_from_minio():
#     env_settings = EnvironmentSettings.in_batch_mode()
#     t_env = TableEnvironment.create(env_settings)

#     # MinIO config
#     config = t_env.get_config().get_configuration()
#     config.set_string("s3.endpoint", "http://minio:9000")
#     config.set_string("s3.access-key", "admin")
#     config.set_string("s3.secret-key", "admin123")
#     config.set_string("s3.path.style.access", "true")
#     config.set_string("table.exec.resource.default-parallelism", "1")

#     for ticker in TOP_30_TICKERS:
#         path = f"s3a://company-fundamentals/{ticker}/2024.parquet"
#         try:
#             t_env.execute_sql(f"""
#                 CREATE TEMPORARY TABLE fundamentals_{ticker} (
#                     eps DOUBLE,
#                     cashflow_freeCashFlow DOUBLE,
#                     revenue DOUBLE,
#                     netIncome DOUBLE,
#                     balance_totalDebt DOUBLE,
#                     balance_totalStockholdersEquity DOUBLE
#                 ) WITH (
#                     'connector' = 'filesystem',
#                     'path' = '{path}',
#                     'format' = 'parquet',
#                     'parquet.ignore-unknown-fields' = 'true'
#                 )
#             """)

#             table = t_env.from_path(f"fundamentals_{ticker}")
#             result = table.execute().collect()
#             for row in result:
#                 eps = row["eps"]
#                 fcf = row["cashflow_freeCashFlow"]
#                 revenue = row["revenue"]
#                 net_income = row["netIncome"]
#                 debt = row["balance_totalDebt"]
#                 equity = row["balance_totalStockholdersEquity"]

#                 profit_margin = net_income / revenue if revenue not in [None, 0] else None
#                 debt_to_equity = debt / equity if equity not in [None, 0] else None

#                 fundamentals_data[ticker] = {
#                     "eps": eps,
#                     "freeCashFlow": fcf,
#                     "profit_margin": profit_margin,
#                     "debt_to_equity": debt_to_equity
#                 }
#                 break  # stop after first row
#         except Exception as e:
#             print(f"[WARN] Could not load fundamentals for {ticker}: {e}", file=sys.stderr)

# class SlidingAggregator(KeyedProcessFunction):
#     def open(self, ctx: RuntimeContext):
#         def descriptor(name): return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())
#         self.price_1m = ctx.get_map_state(descriptor("price_1m"))
#         self.price_5m = ctx.get_map_state(descriptor("price_5m"))
#         self.price_30m = ctx.get_map_state(descriptor("price_30m"))
#         self.size_1m = ctx.get_map_state(descriptor("size_1m"))
#         self.size_5m = ctx.get_map_state(descriptor("size_5m"))
#         self.size_30m = ctx.get_map_state(descriptor("size_30m"))
#         self.last_timer_state = ctx.get_state(ValueStateDescriptor("last_timer", Types.LONG()))

#     def _cleanup_old_entries(self, state, minutes):
#         threshold = datetime.now(timezone.utc) - timedelta(minutes=minutes)
#         for k in list(state.keys()):
#             try:
#                 if datetime.fromisoformat(k) < threshold:
#                     state.remove(k)
#             except:
#                 state.remove(k)

#     def process_element(self, value, ctx):
#         try:
#             data = json.loads(value)

#             if "alias" in data and "value" in data:
#                 alias_key = data["alias"]
#                 if alias_key in macro_alias:
#                     macro_data_dict[macro_alias[alias_key]] = float(data["value"])
#                 return

#             ticker = data.get("ticker")
#             if ticker not in TOP_30_TICKERS:
#                 return

#             ctx.set_current_key(ticker)
#             ts_str = data["timestamp"]
#             price = float(data["price"])
#             size = float(data["size"])

#             for s in [self.price_1m, self.price_5m, self.price_30m]: s.put(ts_str, price)
#             for s in [self.size_1m, self.size_5m, self.size_30m]: s.put(ts_str, size)

#             for s, m in [(self.price_1m, 1), (self.price_5m, 5), (self.price_30m, 30),
#                          (self.size_1m, 1), (self.size_5m, 5), (self.size_30m, 30)]:
#                 self._cleanup_old_entries(s, m)

#             if self.last_timer_state.value() is None:
#                 next_ts = ctx.timer_service().current_processing_time() + 60000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)

#         except Exception as e:
#             print(f"[ERROR] process_element: {e}", file=sys.stderr)

#     def on_timer(self, timestamp, ctx):
#         try:
#             now = datetime.now(timezone.utc)
#             ts_str = now.isoformat()
#             ticker = ctx.get_current_key()

#             def mean(vals): v = list(vals); return float(np.mean(v)) if v else None
#             def std(vals): v = list(vals); return float(np.std(v)) if v else None
#             def total(vals): v = list(vals); return float(np.sum(v)) if v else 0.0

#             features = {
#                 "ticker": ticker,
#                 "timestamp": ts_str,
#                 "price_mean_1min": mean(self.price_1m.values()),
#                 "price_mean_5min": mean(self.price_5m.values()),
#                 "price_std_5min": std(self.price_5m.values()),
#                 "price_mean_30min": mean(self.price_30m.values()),
#                 "price_std_30min": std(self.price_30m.values()),
#                 "size_tot_1min": total(self.size_1m.values()),
#                 "size_tot_5min": total(self.size_5m.values()),
#                 "size_tot_30min": total(self.size_30m.values()),
#             }

#             fundamentals = fundamentals_data.get(ticker, {})
#             features["eps"] = fundamentals.get("eps")
#             features["freeCashFlow"] = fundamentals.get("freeCashFlow")
#             features["profit_margin"] = fundamentals.get("profit_margin")
#             features["debt_to_equity"] = fundamentals.get("debt_to_equity")

#             for macro_key in macro_alias.values():
#                 features[macro_key] = macro_data_dict.get(macro_key)

#             result = json.dumps(features)
#             print(f"[SENT] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             next_ts = ctx.timer_service().current_processing_time() + 5000
#             ctx.timer_service().register_processing_time_timer(next_ts)
#             self.last_timer_state.update(next_ts)

#             return result
#         except Exception as e:
#             print(f"[ERROR] on_timer: {e}", file=sys.stderr)

# def route_by_ticker(json_str):
#     try:
#         data = json.loads(json_str)
#         return data.get("ticker") or "unknown"
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     load_fundamentals_from_minio()

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_group',
#         'auto.offset.reset': 'earliest'
#     }

#     consumer_stock = FlinkKafkaConsumer(
#         topics=["stock_trades"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     consumer_macro = FlinkKafkaConsumer(
#         topics=["macrodata"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregated-data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     stream_stock = env.add_source(consumer_stock, type_info=Types.STRING())
#     stream_macro = env.add_source(consumer_macro, type_info=Types.STRING())

#     merged_stream = stream_stock.union(stream_macro)
#     keyed = merged_stream.key_by(route_by_ticker, key_type=Types.STRING())
#     processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
#     processed.add_sink(producer)

#     env.execute("Full Aggregation with Sliding Windows and Macrodata")

# if __name__ == "__main__":
#     main()
