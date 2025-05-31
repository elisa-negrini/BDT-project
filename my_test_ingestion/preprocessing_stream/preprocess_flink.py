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
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, CoProcessFunction
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from pyflink.datastream.output_tag import OutputTag
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

# # Global dictionaries for shared state (read-only after load)
# fundamentals_data = {}

# NY_TZ = pytz.timezone('America/New_York')

# MINIO_URL = "minio:9000"
# MINIO_ACCESS_KEY = "admin"
# MINIO_SECRET_KEY = "admin123"
# MINIO_SECURE = False

# def load_fundamental_data():
#     print("🚀 [INIT] Loading fundamental data from MinIO...", file=sys.stderr)
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
#                     print(f"✅ [FUNDAMENTALS] Loaded data for {ticker}: {fundamentals_data[ticker]}", file=sys.stderr)
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
#         print("✅ [INIT] Fundamental data loading complete.", file=sys.stderr)
#         # DEBUG: Verify a sample of loaded fundamental data types
#         for ticker, data in list(fundamentals_data.items())[:2]: # Print only first 2 for brevity
#             print(f"[DEBUG_FUNDAMENTALS_SAMPLE] {ticker}: {data}", file=sys.stderr)
#             for k, v in data.items():
#                 print(f"  - {k}: {v} (Type: {type(v)})", file=sys.stderr)

#     except Exception as e:
#         print(f"[CRITICAL] Failed to initialize Minio client or load any fundamental data: {e}", file=sys.stderr)

# # Output Tag per il sentiment generale
# GENERAL_SENTIMENT_OUTPUT_TAG = OutputTag("general_sentiment_output", Types.STRING())

# class SlidingAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         print(f"⚙️ [SlidingAggregator] Opening for key: {runtime_context.get_current_key()}", file=sys.stderr)
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         self.price_1m = runtime_context.get_map_state(descriptor("price_1m"))
#         self.price_5m = runtime_context.get_map_state(descriptor("price_5m"))
#         self.price_30m = runtime_context.get_map_state(descriptor("price_30m"))

#         self.size_1m = runtime_context.get_map_state(descriptor("size_1m"))
#         self.size_5m = runtime_context.get_map_state(descriptor("size_5m"))
#         self.size_30m = runtime_context.get_map_state(descriptor("size_30m"))

#         self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
#         self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

#         self.sentiment_bluesky_general_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_general_2h"))
#         self.sentiment_bluesky_general_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_general_1d"))

#         self.last_timer_state = runtime_context.get_state(
#             ValueStateDescriptor("last_timer", Types.LONG()))
#         print(f"✅ [SlidingAggregator] Open complete for key: {runtime_context.get_current_key()}", file=sys.stderr)


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
#                 print(f"[WARN_CLEANUP] Invalid timestamp format '{k}' in state for cleanup. Removing.", file=sys.stderr)
#                 keys_to_remove.append(k)
#             except Exception as e:
#                 print(f"[ERROR_CLEANUP] Unexpected error during cleanup for key '{k}': {e}. Removing.", file=sys.stderr)
#                 keys_to_remove.append(k)
        
#         if keys_to_remove:
#             print(f"[DEBUG_CLEANUP] Removing {len(keys_to_remove)} old entries from state (window {window_minutes}m) for key {self.get_runtime_context().get_current_key()}", file=sys.stderr)
#         for k_remove in keys_to_remove:
#             state.remove(k_remove)

#     def process_element(self, value, ctx):
#         """Processes each incoming element (JSON string) from Kafka."""
#         current_key = ctx.get_current_key()
#         print(f"📥 [SlidingAggregator] Processing element for key '{current_key}': {value}", file=sys.stderr)
#         try:
#             data = json.loads(value)
#             print(f"[DEBUG_PROCESS_ELEMENT] Parsed data for '{current_key}': {data}", file=sys.stderr)

#             # --- Handle Macro Data ---
#             if current_key == "macro_data_key":
#                 print(f"📥 [MACRO] Received macro data for broadcast: {data.get('alias')}: {data.get('value')}", file=sys.stderr)
#                 return [] 

#             # --- Handle Sentiment Data (specific tickers or GENERAL) ---
#             elif "social" in data and "sentiment_score" in data:
#                 social_source = data.get("social")
#                 sentiment_score = float(data.get("sentiment_score"))
#                 ts_str = data.get("timestamp")
                
#                 if not ts_str:
#                     print(f"[ERROR_SENTIMENT] Missing timestamp in sentiment data: {data}", file=sys.stderr)
#                     return []

#                 if current_key == GENERAL_SENTIMENT_KEY:
#                     if social_source == "bluesky":
#                         self.sentiment_bluesky_general_2h.put(ts_str, sentiment_score)
#                         self.sentiment_bluesky_general_1d.put(ts_str, sentiment_score)
#                         print(f"📥 [SENTIMENT-GENERAL] Bluesky - {ts_str}: {sentiment_score}", file=sys.stderr)
#                     else:
#                         print(f"[WARN_SENTIMENT] General sentiment received for non-Bluesky source: {social_source}", file=sys.stderr)
#                     return []
                
#                 elif current_key in TOP_30_TICKERS:
#                     if social_source == "bluesky":
#                         self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
#                         self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
#                     elif social_source == "news":
#                         self.sentiment_news_1d.put(ts_str, sentiment_score)
#                         self.sentiment_news_3d.put(ts_str, sentiment_score)
#                     else:
#                         print(f"[WARN_SENTIMENT] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
#                         return []
#                     print(f"📥 [SENTIMENT-TICKER] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr)
#                 return []

#             # --- Handle Stock Trade Data ---
#             elif "price" in data and "size" in data:
#                 ticker = data.get("ticker")
#                 if ticker not in TOP_30_TICKERS:
#                     print(f"[DEBUG_TRADE] Skipping trade for untracked ticker: {ticker}", file=sys.stderr)
#                     return []
                
#                 ts_str = data.get("timestamp")
#                 if not ts_str:
#                     print(f"[ERROR_TRADE] Missing timestamp in trade data: {data}", file=sys.stderr)
#                     return []

#                 try:
#                     event_time_utc = isoparse(ts_str).replace(tzinfo=timezone.utc)
#                     event_time_ny = event_time_utc.astimezone(NY_TZ)
#                 except Exception as parse_e:
#                     print(f"[ERROR_TRADE] Failed to parse timestamp '{ts_str}': {parse_e}", file=sys.stderr)
#                     return []

#                 market_open_data_reception = event_time_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#                 market_close_data_reception = event_time_ny.replace(hour=16, minute=0, second=0, microsecond=0)

#                 if not (market_open_data_reception <= event_time_ny < market_close_data_reception):
#                     print(f"[DEBUG_TRADE] Skipping trade data for {ticker} outside market reception hours ({event_time_ny.strftime('%H:%M')} NYT).", file=sys.stderr)
#                     return []

#                 price = float(data.get("price"))
#                 size = float(data.get("size"))
#                 print(f"📥 [TRADE] {ticker} - {ts_str}: Price={price}, Size={size}", file=sys.stderr)

#                 for state in [self.price_1m, self.price_5m, self.price_30m]:
#                     state.put(ts_str, price)
#                 for state in [self.size_1m, self.size_5m, self.size_30m]:
#                     state.put(ts_str, size)
#                 return []

#             else:
#                 print(f"[WARN_PROCESS_ELEMENT] Unrecognized data format for '{current_key}': {value}", file=sys.stderr)
#                 return []

#         except json.JSONDecodeError:
#             print(f"[ERROR_PROCESS_ELEMENT] Failed to decode JSON for '{current_key}': {value}", file=sys.stderr)
#             return []
#         except Exception as e:
#             print(f"[ERROR_PROCESS_ELEMENT_GENERIC] Current Key: '{current_key}', Error: {e} for value: {value}", file=sys.stderr)
#             return []
#         finally:
#             # Always register a timer if none is set or if the current time has passed the last timer.
#             current_processing_time = ctx.timer_service().current_processing_time()
#             last_timer = self.last_timer_state.value()
            
#             # Schedule timer for 5 seconds from now, or if current time is past last timer + buffer
#             if last_timer is None or current_processing_time >= last_timer + 5000: 
#                 next_ts = current_processing_time + 5000 # 5 seconds
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)
#                 print(f"[DEBUG_TIMER] Registered timer for {current_key} at {datetime.fromtimestamp(next_ts / 1000, tz=timezone.utc).isoformat()}", file=sys.stderr)


#     def on_timer(self, timestamp, ctx):
#         """Called when a registered timer fires."""
#         ticker = ctx.get_current_key()
#         print(f"⏰ [SlidingAggregator] Timer fired for key: {ticker} at {datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).isoformat()}", file=sys.stderr)
#         try:
#             now_utc = datetime.now(timezone.utc)
#             ts_str = now_utc.isoformat()

#             def mean(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else 0.0

#             def std(vals):
#                 vals = list(vals)
#                 return float(np.std(vals)) if vals and len(vals) > 1 else 0.0
            
#             def total(vals):
#                 vals = list(vals)
#                 return float(np.sum(vals)) if vals else 0.0

#             # --- Handle GENERAL Sentiment Key ---
#             if ticker == GENERAL_SENTIMENT_KEY:
#                 print(f"[DEBUG_TIMER_GENERAL] Cleaning up general sentiment state for {ticker}", file=sys.stderr)
#                 self._cleanup_old_entries(self.sentiment_bluesky_general_2h, 2 * 60)
#                 self._cleanup_old_entries(self.sentiment_bluesky_general_1d, 24 * 60)

#                 current_general_mean_2h = mean(self.sentiment_bluesky_general_2h.values())
#                 current_general_mean_1d = mean(self.sentiment_bluesky_general_1d.values())
                
#                 general_sentiment_output = {
#                     "key": GENERAL_SENTIMENT_KEY,
#                     "timestamp": ts_str,
#                     "sentiment_bluesky_mean_general_2hours": current_general_mean_2h,
#                     "sentiment_bluesky_mean_general_1d": current_general_mean_1d
#                 }
                
#                 ctx.output(GENERAL_SENTIMENT_OUTPUT_TAG, json.dumps(general_sentiment_output))
#                 print(f"🔄 [GENERAL SENTIMENT AGG] Emitted to side output: {general_sentiment_output}", file=sys.stderr)
#                 return []

#             # --- Handle Macro Data Key ---
#             if ticker == "macro_data_key":
#                 print(f"[DEBUG_TIMER_MACRO] Timer fired for macro_data_key. No aggregation needed here.", file=sys.stderr)
#                 return []

#             # --- Handle Specific Ticker Data (TOP_30_TICKERS) ---
#             if ticker not in TOP_30_TICKERS:
#                 print(f"[WARN_TIMER] on_timer fired for unexpected key: {ticker}", file=sys.stderr)
#                 return []

#             now_ny = now_utc.astimezone(NY_TZ)

#             market_open_prediction = now_ny.replace(hour=9, minute=31, second=0, microsecond=0)
#             market_close_prediction = now_ny.replace(hour=15, minute=59, second=0, microsecond=0)

#             if not (market_open_prediction <= now_ny <= market_close_prediction):
#                 print(f"[DEBUG_TIMER] Skipping prediction for {ticker} outside market prediction hours ({now_ny.strftime('%H:%M')} NYT).", file=sys.stderr)
#                 return []

#             print(f"[DEBUG_TIMER] Cleaning up states for {ticker}...", file=sys.stderr)
#             self._cleanup_old_entries(self.price_1m, 1)
#             self._cleanup_old_entries(self.price_5m, 5)
#             self._cleanup_old_entries(self.price_30m, 30)
#             self._cleanup_old_entries(self.size_1m, 1)
#             self._cleanup_old_entries(self.size_5m, 5)
#             self._cleanup_old_entries(self.size_30m, 30)
#             self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
#             self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)
#             print(f"[DEBUG_TIMER] State cleanup complete for {ticker}.", file=sys.stderr)

#             market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#             market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)

#             market_open_spike_flag = 0
#             market_close_spike_flag = 0

#             if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
#                 market_open_spike_flag = 1
            
#             if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
#                 market_close_spike_flag = 1

#             ticker_fundamentals = fundamentals_data.get(ticker, {})
#             print(f"[DEBUG_TIMER] Fundamentals for {ticker}: {ticker_fundamentals}", file=sys.stderr)


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
#                 "sentiment_bluesky_mean_2h": mean(self.sentiment_bluesky_2h.values()),
#                 "sentiment_bluesky_mean_1d": mean(self.sentiment_bluesky_1d.values()),
#                 "sentiment_news_mean_1d": mean(self.sentiment_news_1d.values()),
#                 "sentiment_news_mean_3d": mean(self.sentiment_news_3d.values()),
#                 "minutes_since_open": int((now_ny - market_open_time).total_seconds() // 60) if now_ny >= market_open_time else -1,
#                 "day_of_week": int(now_ny.weekday()),
#                 "day_of_month": int(now_ny.day),
#                 "week_of_year": int(now_ny.isocalendar()[1]),
#                 "month_of_year": int(now_ny.month),
#                 "market_open_spike_flag": int(market_open_spike_flag),
#                 "market_close_spike_flag": int(market_close_spike_flag),
#                 "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
#                 "freeCashFlow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
#                 "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
#                 "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None
#             }
#             print(f"[DEBUG_TIMER] Raw features for {ticker}: {features}", file=sys.stderr)

#             # Return the features for further processing by the CoProcessFunction
#             return [json.dumps(features)]

#         except Exception as e:
#             print(f"[ERROR_ON_TIMER] For ticker {ticker}, Error: {e}", file=sys.stderr)
#             # Emit an error payload or skip, based on your error handling strategy
#             return [json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)})]


# # Define BroadcastStateDescriptor for the general sentiment and macro data
# # THIS MUST BE DEFINED GLOBALLY SO FeatureJoiner CAN ACCESS IT AT CLASS DEFINITION TIME
# BROADCAST_STATE_DESCRIPTOR = MapStateDescriptor(
#     "merged_broadcast_state",
#     Types.STRING(),
#     Types.FLOAT()
# )

# def clean_features_for_json(features_dict):
#     """
#     Converts None values to 0.0 and ensures all numeric types are standard Python floats/ints
#     to prevent serialization issues with json.dumps.
#     """
#     cleaned = {}
#     for k, v in features_dict.items():
#         if v is None:
#             cleaned[k] = 0.0 # Replace None with 0.0
#         elif isinstance(v, (np.float_, np.float16, np.float32, np.float64)):
#             cleaned[k] = float(v)
#         elif isinstance(v, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
#             cleaned[k] = int(v) # Or float(v) if all numeric types should be float
#         else:
#             cleaned[k] = v
#     return cleaned

# class FeatureJoiner(CoProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         print(f"⚙️ [FeatureJoiner] Opening...", file=sys.stderr)
#         self.broadcast_state = runtime_context.get_map_state(BROADCAST_STATE_DESCRIPTOR)
#         print(f"✅ [FeatureJoiner] Open complete.", file=sys.stderr)

#     def process_element1(self, value, ctx):
#         """Processes elements from the main stream (ticker-specific features)."""
#         print(f"📥 [FeatureJoiner-1] Processing element1 (ticker features): {value}", file=sys.stderr)
#         try:
#             features = json.loads(value)
#             print(f"[DEBUG_JOINER_INPUT] Parsed ticker features: {features}", file=sys.stderr)
            
#             # Add general sentiment from broadcast state
#             general_sentiment_2h = self.broadcast_state.get("sentiment_bluesky_mean_general_2hours")
#             general_sentiment_1d = self.broadcast_state.get("sentiment_bluesky_mean_general_1d")

#             features["sentiment_bluesky_mean_general_2hours"] = general_sentiment_2h if general_sentiment_2h is not None else 0.0
#             features["sentiment_bluesky_mean_general_1d"] = general_sentiment_1d if general_sentiment_1d is not None else 0.0

#             # Add macro data from broadcast state
#             for alias_key in macro_alias.values():
#                 macro_val = self.broadcast_state.get(alias_key)
#                 features[alias_key] = macro_val if macro_val is not None else 0.0
            
#             print(f"[DEBUG_JOINER_MERGED] Merged features (before clean): {features}", file=sys.stderr)

#             # Clean features before final JSON serialization
#             cleaned_features = clean_features_for_json(features)
#             print(f"[DEBUG_JOINER_CLEANED] Merged features (after clean): {cleaned_features}", file=sys.stderr)
            
#             result = json.dumps(cleaned_features)
#             print(f"📤 [FINAL_OUTPUT] {cleaned_features.get('timestamp')} - {cleaned_features.get('ticker')} => {result}", file=sys.stderr)
#             yield result # Emit to the final sink
#         except json.JSONDecodeError:
#             print(f"[ERROR_JOINER_1] Failed to decode JSON in FeatureJoiner (element1): {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"[ERROR_JOINER_1_GENERIC] Error in FeatureJoiner process_element1: {e} for value: {value}", file=sys.stderr)


#     def process_element2(self, value, ctx):
#         """Processes elements from the broadcast stream (general sentiment or macro data)."""
#         print(f"📥 [FeatureJoiner-2] Processing element2 (broadcast data): {value}", file=sys.stderr)
#         try:
#             data = json.loads(value)
#             print(f"[DEBUG_JOINER_2] Parsed broadcast data: {data}", file=sys.stderr)
            
#             if data.get("key") == GENERAL_SENTIMENT_KEY:
#                 # Update general sentiment broadcast state
#                 sentiment_2h = data.get("sentiment_bluesky_mean_general_2hours")
#                 sentiment_1d = data.get("sentiment_bluesky_mean_general_1d")
                
#                 # Ensure values are float before putting into MapState<String, Float>
#                 if sentiment_2h is not None:
#                     self.broadcast_state.put("sentiment_bluesky_mean_general_2hours", float(sentiment_2h))
#                 if sentiment_1d is not None:
#                     self.broadcast_state.put("sentiment_bluesky_mean_general_1d", float(sentiment_1d))
#                 print(f"[BROADCAST] Updated general sentiment broadcast state: 2h={sentiment_2h}, 1d={sentiment_1d}", file=sys.stderr)
#             elif data.get("key") == "macro_data_key":
#                 alias = data.get("alias")
#                 macro_value = data.get("value")
#                 if alias and macro_value is not None:
#                     # Ensure value is float before putting into MapState<String, Float>
#                     self.broadcast_state.put(alias, float(macro_value))
#                     print(f"[BROADCAST] Updated macro data broadcast state: {alias}={macro_value}", file=sys.stderr)
#             else:
#                 print(f"[WARN_JOINER_2] Unrecognized broadcast data key: {data.get('key')} from value: {value}", file=sys.stderr)

#         except json.JSONDecodeError:
#             print(f"[ERROR_JOINER_2] Failed to decode JSON in FeatureJoiner (element2): {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"[ERROR_JOINER_2_GENERIC] Error in FeatureJoiner process_element2: {e} for value: {value}", file=sys.stderr)

# # --- Helper for splitting sentiment data ---
# def expand_sentiment_data(json_str):
#     """
#     Expands a single sentiment JSON string into multiple if it contains a list of tickers,
#     otherwise passes through other data types.
#     Handles 'GENERAL' ticker by passing it through.
#     """
#     print(f"➡️ [expand_sentiment_data] Input: {json_str}", file=sys.stderr)
#     try:
#         data = json.loads(json_str)
        
#         if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
#             expanded_records = []
#             original_ticker_list = data["ticker"]
            
#             if "GENERAL" in original_ticker_list:
#                 new_record = data.copy()
#                 new_record["ticker"] = "GENERAL"
#                 expanded_records.append(json.dumps(new_record))
#                 print(f"[DEBUG_EXPAND] Expanded 'GENERAL' sentiment: {new_record}", file=sys.stderr)
            
#             for ticker_item in original_ticker_list:
#                 if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
#                     new_record = data.copy()
#                     new_record["ticker"] = ticker_item
#                     expanded_records.append(json.dumps(new_record))
#                     print(f"[DEBUG_EXPAND] Expanded specific ticker sentiment: {new_record}", file=sys.stderr)
            
#             if not expanded_records:
#                 print(f"[WARN_EXPAND] Sentiment data with no tracked or 'GENERAL' tickers after expansion: {json_str}", file=sys.stderr)
#                 return []
#             return expanded_records
        
#         print(f"[DEBUG_EXPAND] Passing through non-list ticker data: {json_str}", file=sys.stderr)
#         return [json_str]
#     except json.JSONDecodeError:
#         print(f"[ERROR_EXPAND] Failed to decode JSON in expand_sentiment_data: {json_str}", file=sys.stderr)
#         return []
#     except Exception as e:
#         print(f"[ERROR_EXPAND_GENERIC] expand_sentiment_data: {e} for {json_str}", file=sys.stderr)
#         return []

# def route_by_ticker(json_str):
#     """Determines the key for incoming JSON data."""
#     print(f"🔑 [route_by_ticker] Routing input: {json_str}", file=sys.stderr)
#     try:
#         data = json.loads(json_str)
#         if "alias" in data:
#             print(f"[DEBUG_ROUTE] Routed to 'macro_data_key'", file=sys.stderr)
#             return "macro_data_key"
#         elif "ticker" in data:
#             if data["ticker"] == "GENERAL":
#                 print(f"[DEBUG_ROUTE] Routed to 'GENERAL_SENTIMENT_KEY'", file=sys.stderr)
#                 return GENERAL_SENTIMENT_KEY
#             elif data["ticker"] in TOP_30_TICKERS:
#                 print(f"[DEBUG_ROUTE] Routed to ticker '{data['ticker']}'", file=sys.stderr)
#                 return data["ticker"]
#             else:
#                 print(f"[WARN_ROUTE] Ticker '{data['ticker']}' not in TOP_30_TICKERS. Discarding.", file=sys.stderr)
#                 return "discard_key"
#         else:
#             print(f"[WARN_ROUTE] Data with no 'alias' or 'ticker' field: {json_str}", file=sys.stderr)
#             return "unknown_data_key"
#     except json.JSONDecodeError:
#         print(f"[WARN_ROUTE] Failed to decode JSON for key_by: {json_str}", file=sys.stderr)
#         return "invalid_json_key"
#     except Exception as e:
#         print(f"[ERROR_ROUTE_GENERIC] route_by_ticker: {e} for {json_str}", file=sys.stderr)
#         return "error_key"


# def main():
#     print("🚀 [MAIN] Starting Flink job setup...", file=sys.stderr)
#     load_fundamental_data()

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)  # Aumenta per produzione
#     env.enable_checkpointing(60000)
#     print("✅ [MAIN] Flink Environment configured.", file=sys.stderr)

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
#     print("✅ [MAIN] Kafka Consumer configured.", file=sys.stderr)

#     producer_aggregated_data = FlinkKafkaProducer(
#         topic='aggregated-data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )
#     producer_general_sentiment = FlinkKafkaProducer(
#         topic='bluesky_general_sentiment',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )
#     print("✅ [MAIN] Kafka Producers configured.", file=sys.stderr)

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     print("✅ [MAIN] Added Kafka source.", file=sys.stderr)

#     expanded_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())
#     print("✅ [MAIN] Added expand_sentiment_data flat_map.", file=sys.stderr)

#     keyed_stream = expanded_stream.key_by(route_by_ticker, key_type=Types.STRING())
#     print("✅ [MAIN] Added route_by_ticker key_by.", file=sys.stderr)

#     processed_with_side_outputs = keyed_stream.process(SlidingAggregator(), output_type=Types.STRING())
#     main_ticker_features_stream = processed_with_side_outputs
#     print("✅ [MAIN] Added SlidingAggregator process function.", file=sys.stderr)

#     general_sentiment_stream = processed_with_side_outputs.get_side_output(GENERAL_SENTIMENT_OUTPUT_TAG)
#     general_sentiment_stream.add_sink(producer_general_sentiment)
#     print("✅ [MAIN] General sentiment side output configured to Kafka.", file=sys.stderr)

#     general_sentiment_consumer = FlinkKafkaConsumer(
#         topics=["bluesky_general_sentiment"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             'bootstrap.servers': 'kafka:9092',
#             'group.id': 'flink_general_sentiment_broadcast_group',
#             'auto.offset.reset': 'latest'
#         }
#     )
#     general_sentiment_data_stream = env.add_source(general_sentiment_consumer, type_info=Types.STRING())
#     print("✅ [MAIN] General sentiment broadcast consumer configured.", file=sys.stderr)

#     # Estrai e formatta la macrodata dallo stream principale
#     macro_data_stream = expanded_stream.filter(lambda x: json.loads(x).get("alias") is not None)
#     macro_data_formatted = macro_data_stream.map(
#         lambda x: json.dumps({
#             "key": "macro_data_key",
#             "alias": json.loads(x)["alias"],
#             "value": json.loads(x)["value"]
#         }),
#         output_type=Types.STRING()
#     )
#     print("✅ [MAIN] Macro data extraction and formatting configured.", file=sys.stderr)

#     merged_broadcast_data_stream = general_sentiment_data_stream.union(macro_data_formatted)
#     print("✅ [MAIN] Merged general sentiment and macro data for broadcast.", file=sys.stderr)

#     merged_broadcast_stream = merged_broadcast_data_stream.broadcast(BROADCAST_STATE_DESCRIPTOR)
#     print("✅ [MAIN] Broadcast stream created.", file=sys.stderr)

#     final_features_stream = main_ticker_features_stream \
#         .connect(merged_broadcast_stream) \
#         .process(FeatureJoiner(), output_type=Types.STRING())
#     print("✅ [MAIN] FeatureJoiner connected to main and broadcast streams.", file=sys.stderr)

#     final_features_stream.add_sink(producer_aggregated_data)
#     print("✅ [MAIN] Final aggregated data sink to Kafka configured.", file=sys.stderr)

#     print("🚀 [MAIN] Executing Flink job...", file=sys.stderr)
#     env.execute("Full Aggregation with Sliding Windows, Macrodata, Sentiment, Time, and Fundamental Features")
#     print("✅ [MAIN] Flink job execution finished.", file=sys.stderr)


# if __name__ == "__main__":
#     main()

