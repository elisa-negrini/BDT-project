------------------ script ultimo
"""
Flink Real-Time Financial Preprocessor

This module sets up a PyFlink streaming job for processing real-time financial data
from multiple Kafka topics, aggregating metrics like prices, volumes, and sentiment,
and computing derived features for downstream analytics and ML models.

Features:
- Ingests financial trade, sentiment, and macroeconomic data from Kafka
- Maintains windowed state using Flink MapState
- Performs time-based aggregation and cleanup
- Outputs feature vectors to Kafka for downstream consumption

Dependencies:
- Apache Flink (PyFlink)
- Kafka
"""

import sys
import json
import math
import numpy as np
from datetime import datetime, timezone, timedelta, time as dtime
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor

# Macro alias mapping
macro_alias = {
    "GDPC1": "gdp_real",
    "CPIAUCSL": "cpi",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment"
}

# Static list of top 30 tickers
TOP_30_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

class Aggregator(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        self._initialize_states(ctx)
        self.last_timer = None

    def _initialize_states(self, ctx):
        def descriptor(name): return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

        try:
            self.price_state_1min = ctx.get_map_state(descriptor("price_state_1min"))
            self.price_state_5min = ctx.get_map_state(descriptor("price_state_5min"))
            self.price_state_30min = ctx.get_map_state(descriptor("price_state_30min"))
            self.size_state_1min = ctx.get_map_state(descriptor("size_state_1min"))
            self.size_state_5min = ctx.get_map_state(descriptor("size_state_5min"))
            self.size_state_30min = ctx.get_map_state(descriptor("size_state_30min"))

            self.sentiment_bluesky_state_2h = ctx.get_map_state(descriptor("sentiment_bluesky_state_2h"))
            self.sentiment_bluesky_state_1d = ctx.get_map_state(descriptor("sentiment_bluesky_state_1d"))
            # self.sentiment_reddit_state_2h = ctx.get_map_state(descriptor("sentiment_reddit_state_2h"))
            # self.sentiment_reddit_state_1d = ctx.get_map_state(descriptor("sentiment_reddit_state_1d"))
            self.sentiment_news_state_1d = ctx.get_map_state(descriptor("sentiment_news_state_1d"))
            self.sentiment_news_state_3d = ctx.get_map_state(descriptor("sentiment_news_state_3d"))

            self.sentiment_general_bluesky_state_2h = ctx.get_map_state(descriptor("sentiment_general_bluesky_state_2h"))
            self.sentiment_general_bluesky_state_1d = ctx.get_map_state(descriptor("sentiment_general_bluesky_state_1d"))
            # self.sentiment_general_reddit_state_2h = ctx.get_map_state(descriptor("sentiment_general_reddit_state_2h"))
            # self.sentiment_general_reddit_state_1d = ctx.get_map_state(descriptor("sentiment_general_reddit_state_1d"))

            self.macro_state = ctx.get_map_state(descriptor("macro_state"))
        except Exception as e:
            print(f"[ERROR] Failed to initialize state: {e}", file=sys.stderr)

    def _cleanup_old_entries(self, state, window_minutes):
        threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        try:
            for k in list(state.keys()):
                try:
                    if datetime.fromisoformat(k) < threshold:
                        state.remove(k)
                except:
                    state.remove(k)
        except Exception as e:
            print(f"[ERROR] cleanup_old_entries failed: {e}", file=sys.stderr)


    def process_element(self, value, ctx):
        try:
            print(f"[DEBUG] Incoming value: {value}", file=sys.stderr)
            data = json.loads(value)
            ts_str = data["timestamp"]
            now = datetime.fromisoformat(ts_str)
            key = ctx.get_current_key()

            if not (dtime(13, 31) <= now.time() <= dtime(20, 0)):
                return

            if data.get("ticker") == "GENERAL":
                for ticker in TOP_30_TICKERS:
                    data_copy = data.copy()
                    data_copy["ticker"] = ticker
                    self._handle_input(data_copy, ts_str)
                    self._handle_general_sentiment(data_copy, ts_str)
            else:
                self._handle_input(data, ts_str)

            if self.last_timer is None:
                next_ts = ctx.timer_service().current_processing_time() + 1000
                ctx.timer_service().register_processing_time_timer(next_ts)
                self.last_timer = next_ts

        except Exception as e:
            print(f"[ERROR] process_element: {e}", file=sys.stderr)

    def _handle_input(self, data, ts_str):
        try: 
            if "price" in data:
                price = float(data["price"])
                size = float(data["size"])
                print(f"[DEBUG] price={price} size={size} type={type(price)}", file=sys.stderr)

                for state in [self.price_state_1min, self.price_state_5min, self.price_state_30min]:
                    try:
                        state.put(ts_str, price)
                    except Exception as e:
                        print(f"[ERROR] state.put(price) failed: {e}", file=sys.stderr)

                for state in [self.size_state_1min, self.size_state_5min, self.size_state_30min]:
                    try:
                        state.put(ts_str, size)
                    except Exception as e:
                        print(f"[ERROR] state.put(size) failed: {e}", file=sys.stderr)

                self._cleanup_price_size()

            elif "sentiment" in data:
                self._handle_sentiment(data, ts_str)

            elif "alias" in data and "value" in data:
                alias = macro_alias.get(data["alias"])
                if alias:
                    try:
                        self.macro_state.put(alias, float(data["value"]))
                    except Exception as e:
                        print(f"[ERROR] macro_state.put failed: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] _handle_input exception: {e}", file=sys.stderr)


    def _handle_general_sentiment(self, data, ts_str):
        try:
            source = data.get("source")
            sentiment_value = float(data["sentiment"])

            def put_and_cleanup(state_2h, state_1d, m2=120, m1=1440):
                try:
                    state_2h.put(ts_str, sentiment_value)
                    state_1d.put(ts_str, sentiment_value)
                    self._cleanup_old_entries(state_2h, m2)
                    self._cleanup_old_entries(state_1d, m1)
                except Exception as e:
                    print(f"[ERROR] sentiment_state.put failed: {e}", file=sys.stderr)


            if source == "bluesky":
                put_and_cleanup(self.sentiment_general_bluesky_state_2h, self.sentiment_general_bluesky_state_1d)
            #elif source == "reddit":
            #    put_and_cleanup(self.sentiment_general_reddit_state_2h, self.sentiment_general_reddit_state_1d)
        except Exception as e:
            print(f"[ERROR] _handle_general_sentiment exception: {e}", file=sys.stderr)


    def _cleanup_price_size(self):
        for state, minutes in zip([
            self.price_state_1min, self.price_state_5min, self.price_state_30min,
            self.size_state_1min, self.size_state_5min, self.size_state_30min
        ], [1, 5, 30, 1, 5, 30]):
            self._cleanup_old_entries(state, minutes)

    def _handle_sentiment(self, data, ts_str):
        try:
            source = data.get("source")
            sentiment_value = float(data["sentiment"])
            tickers = data.get("ticker", [])

            def put_and_cleanup(state_2h, state_1d, m2=120, m1=1440):
                try:
                    state_2h.put(ts_str, sentiment_value)
                    state_1d.put(ts_str, sentiment_value)
                    self._cleanup_old_entries(state_2h, m2)
                    self._cleanup_old_entries(state_1d, m1)
                except Exception as e:
                    print(f"[ERROR] sentiment_state.put failed: {e}", file=sys.stderr)

            if source == "bluesky":
                put_and_cleanup(self.sentiment_bluesky_state_2h, self.sentiment_bluesky_state_1d)
            #elif source == "reddit":
            #    put_and_cleanup(self.sentiment_reddit_state_2h, self.sentiment_reddit_state_1d)
            elif source == "news" and tickers == "general":
                try:
                    self.sentiment_news_state_1d.put(ts_str, sentiment_value)
                    self.sentiment_news_state_3d.put(ts_str, sentiment_value)
                    self._cleanup_old_entries(self.sentiment_news_state_1d, 1440)
                    self._cleanup_old_entries(self.sentiment_news_state_3d, 4320)
                except Exception as e:
                    print(f"[ERROR] news sentiment_state.put failed: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] _handle_sentiment exception: {e}", file=sys.stderr)


    def on_timer(self, timestamp, ctx):
        print(f"[TIMER] Fired at {datetime.now(timezone.utc)}", file=sys.stderr)
        try:
            now = datetime.now(timezone.utc)
            ts_str = now.isoformat()
            key = ctx.get_current_key()

            print(f"[INFO] Output to Kafka - ticker: {key} - timestamp: {ts_str}", file=sys.stderr)

            def mean(vals): return float(np.mean(vals)) if vals else None
            def mean_sent(vals): return float(np.mean(vals)) if vals else 0.0
            def std(vals): return float(np.std(vals)) if vals else None
            def total(vals): return float(np.sum(vals)) if vals else 0.0

            features = {
                "ticker": key,
                "timestamp": ts_str,
                "price_mean_1min": mean(self.price_state_1min.values()),
                "price_mean_5min": mean(self.price_state_5min.values()),
                "price_std_5min": std(self.price_state_5min.values()),
                "price_mean_30min": mean(self.price_state_30min.values()),
                "price_std_30min": std(self.price_state_30min.values()),
                "size_tot_1min": total(self.size_state_1min.values()),
                "size_tot_5min": total(self.size_state_5min.values()),
                "size_tot_30min": total(self.size_state_30min.values()),
                "sentiment_bluesky_mean_2hours": mean_sent(self.sentiment_bluesky_state_2h.values()),
                "sentiment_bluesky_mean_1day": mean_sent(self.sentiment_bluesky_state_1d.values()),
                # "sentiment_reddit_mean_2hours": mean_sent(self.sentiment_reddit_state_2h.values()),
                # "sentiment_reddit_mean_1day": mean_sent(self.sentiment_reddit_state_1d.values()),
                "sentiment_news_mean_1day": mean_sent(self.sentiment_news_state_1d.values()),
                "sentiment_news_mean_3days": mean_sent(self.sentiment_news_state_3d.values()),
                "sentiment_general_bluesky_mean_2hours": mean_sent(self.sentiment_general_bluesky_state_2h.values()),
                "sentiment_general_bluesky_mean_1day": mean_sent(self.sentiment_general_bluesky_state_1d.values()),
                # "sentiment_general_reddit_mean_2hours": mean_sent(self.sentiment_general_reddit_state_2h.values()),
                # "sentiment_general_reddit_mean_1day": mean_sent(self.sentiment_general_reddit_state_1d.values()),
                "minutes_since_open": (now - now.replace(hour=13, minute=30, second=0, microsecond=0)).total_seconds() // 60,
                "day_of_week": now.weekday(),
                "day_of_month": now.day,
                "week_of_year": now.isocalendar()[1],
                "month_of_year": now.month,
                "market_open_spike_flag": int(dtime(13, 30) <= now.time() < dtime(14, 0)),
                "market_close_spike_flag": int(dtime(19, 55) <= now.time() < dtime(20, 0)),
            }

            for k in macro_alias.values():
                features[k] = self.macro_state.get(k) if self.macro_state.contains(k) else None

            ctx.output(json.dumps(features))
            next_ts = ctx.timer_service().current_processing_time() + 5000
            ctx.timer_service().register_processing_time_timer(next_ts)
            self.last_timer = next_ts

        except Exception as e:
            print(f"[ERROR] on_timer: {e}", file=sys.stderr)

def route_by_ticker(json_str):
    try:
        return json.loads(json_str).get("ticker", "unknown")
    except Exception as e:
        print(f"[ERROR] route_by_ticker: {e}", file=sys.stderr)
        return "unknown"

def main():
    print("[INFO] Starting Flink Preprocessing Job", file=sys.stderr)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    env.enable_checkpointing(10000)

    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-preprocessor',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics=["stock_trades", "macrodata", "bluesky_sentiment", "news_sentiment"], # "reddit_sentiment",
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    producer = FlinkKafkaProducer(
        topic="aggregated_data",
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": "kafka:9092"}
    )

    print("[DEBUG] Adding source...", file=sys.stderr)
    stream = env.add_source(consumer)

    print("[DEBUG] Keying by ticker...", file=sys.stderr)
    stream = stream.key_by(route_by_ticker, key_type=Types.STRING())

    print("[DEBUG] Processing with Aggregator...", file=sys.stderr)
    stream = stream.process(Aggregator(), output_type=Types.STRING())

    print("[DEBUG] Adding sink...", file=sys.stderr)
    stream.add_sink(producer)

    print("[DEBUG] Ready to execute", file=sys.stderr)
    env.execute("Unified Flink Preprocessor")

if __name__ == "__main__":
    main()








#script elisa 




# from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor
# from datetime import datetime, timezone, timedelta, time as dtime, date
# import json
# import math
# import numpy as np
# import sys
# import holidays
# from pandas import Timestamp


# macro_alias = {
#     "GDPC1": "gdp_real",
#     "CPIAUCSL": "cpi",
#     "FEDFUNDS": "ffr",
#     "DGS10": "t10y",
#     "DGS2": "t2y",
#     "T10Y2Y": "spread_10y_2y",
#     "UNRATE": "unemployment"
# }

# class Aggregator(KeyedProcessFunction):

#     def open(self, ctx: RuntimeContext):
#         self.price_state_1min = ctx.get_map_state(MapStateDescriptor("price_state_1min", Types.STRING(), Types.FLOAT()))
#         self.price_state_5min = ctx.get_map_state(MapStateDescriptor("price_state_5min", Types.STRING(), Types.FLOAT()))
#         self.price_state_30min = ctx.get_map_state(MapStateDescriptor("price_state_30min", Types.STRING(), Types.FLOAT()))
#         self.size_state_1min = ctx.get_map_state(MapStateDescriptor("size_state_1min", Types.STRING(), Types.FLOAT()))
#         self.size_state_5min = ctx.get_map_state(MapStateDescriptor("size_state_5min", Types.STRING(), Types.FLOAT()))
#         self.size_state_30min = ctx.get_map_state(MapStateDescriptor("size_state_30min", Types.STRING(), Types.FLOAT()))

#         self.sentiment_bluesky_state_2h = ctx.get_map_state(MapStateDescriptor("sentiment_bluesky_state_2h", Types.STRING(), Types.FLOAT()))
#         self.sentiment_bluesky_state_1d = ctx.get_map_state(MapStateDescriptor("sentiment_bluesky_state_1d", Types.STRING(), Types.FLOAT()))
#         self.sentiment_reddit_state_2h = ctx.get_map_state(MapStateDescriptor("sentiment_reddit_state_2h", Types.STRING(), Types.FLOAT()))
#         self.sentiment_reddit_state_1d = ctx.get_map_state(MapStateDescriptor("sentiment_reddit_state_1d", Types.STRING(), Types.FLOAT()))
#         self.sentiment_news_state_1d = ctx.get_map_state(MapStateDescriptor("sentiment_news_state_1d", Types.STRING(), Types.FLOAT()))
#         self.sentiment_news_state_3d = ctx.get_map_state(MapStateDescriptor("sentiment_news_state_3d", Types.STRING(), Types.FLOAT()))

#         # General (per Reddit e Bluesky)
#         self.sentiment_general_bluesky_state_2h = ctx.get_map_state(MapStateDescriptor("sentiment_general_bluesky_state_2h", Types.STRING(), Types.FLOAT()))
#         self.sentiment_general_bluesky_state_1d = ctx.get_map_state(MapStateDescriptor("sentiment_general_bluesky_state_1d", Types.STRING(), Types.FLOAT()))
#         self.sentiment_general_reddit_state_2h = ctx.get_map_state(MapStateDescriptor("sentiment_general_reddit_state_2h", Types.STRING(), Types.FLOAT()))
#         self.sentiment_general_reddit_state_1d = ctx.get_map_state(MapStateDescriptor("sentiment_general_reddit_state_1d", Types.STRING(), Types.FLOAT()))
        
#         self.macro_state = ctx.get_map_state(MapStateDescriptor("macro_state", Types.STRING(), Types.FLOAT()))
#         self.last_timer = None

#     def _cleanup_old_entries(self, state, window_minutes):
#         now = datetime.now(timezone.utc)
#         threshold = now - timedelta(minutes=window_minutes)
#         for k in list(state.keys()):
#             try:
#                 if datetime.fromisoformat(k) < threshold:
#                     state.remove(k)
#             except:
#                 state.remove(k)  # fallback se parsing fallisce

#     def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
#         try:
#             print(f"[DEBUG] Received message: {value}", file=sys.stderr)
#             data = json.loads(value)
#             now = datetime.fromisoformat(data["timestamp"])
#             ts_str = data["timestamp"]
#             key = ctx.get_current_key()

#             if "price" in data:
#                 if not (dtime(13, 31) <= now.time() <= dtime(20, 0)):
#                     print(f"[SKIP] stock_trades fuori orario: {now.time()}", file=sys.stderr)
#                     return
            
#                 price = float(data["price"])
#                 size = float(data["size"])
#                 self.price_state_1min.put(ts_str, price)
#                 self.price_state_5min.put(ts_str, price)
#                 self.price_state_30min.put(ts_str, price)
#                 self.size_state_1min.put(ts_str, size)
#                 self.size_state_5min.put(ts_str, size)
#                 self.size_state_30min.put(ts_str, size)

#                 self._cleanup_old_entries(self.price_state_1min, 1)
#                 self._cleanup_old_entries(self.price_state_5min, 5)
#                 self._cleanup_old_entries(self.price_state_30min, 30)
#                 self._cleanup_old_entries(self.size_state_1min, 1)
#                 self._cleanup_old_entries(self.size_state_5min, 5)
#                 self._cleanup_old_entries(self.size_state_30min, 30)

#             elif "sentiment" in data:
#                 source = data.get("source")
#                 sentiment_value = float(data["sentiment"])
#                 tickers = data.get("ticker", [])
#                 is_general = tickers == "GENERAL" or tickers == ["GENERAL"]

#                 if source == "bluesky":
#                     if is_general:
#                         self.sentiment_general_bluesky_state_2h.put(ts_str, sentiment_value)
#                         self.sentiment_general_bluesky_state_1d.put(ts_str, sentiment_value)
#                         self._cleanup_old_entries(self.sentiment_general_bluesky_state_2h, 120)
#                         self._cleanup_old_entries(self.sentiment_general_bluesky_state_1d, 1440)
#                     else:
#                         self.sentiment_bluesky_state_2h.put(ts_str, sentiment_value)
#                         self.sentiment_bluesky_state_1d.put(ts_str, sentiment_value)
#                         self._cleanup_old_entries(self.sentiment_bluesky_state_2h, 120)
#                         self._cleanup_old_entries(self.sentiment_bluesky_state_1d, 1440)

#                 elif source == "reddit":
#                     if is_general:
#                         self.sentiment_general_reddit_state_2h.put(ts_str, sentiment_value)
#                         self.sentiment_general_reddit_state_1d.put(ts_str, sentiment_value)
#                         self._cleanup_old_entries(self.sentiment_general_reddit_state_2h, 120)
#                         self._cleanup_old_entries(self.sentiment_general_reddit_state_1d, 1440)
#                     else:
#                         self.sentiment_reddit_state_2h.put(ts_str, sentiment_value)
#                         self.sentiment_reddit_state_1d.put(ts_str, sentiment_value)
#                         self._cleanup_old_entries(self.sentiment_reddit_state_2h, 120)
#                         self._cleanup_old_entries(self.sentiment_reddit_state_1d, 1440)

#                 elif source == "news" and data.get("ticker") == "general":
#                     self.sentiment_news_state_1d.put(ts_str, sentiment_value)
#                     self.sentiment_news_state_3d.put(ts_str, sentiment_value)
#                     self._cleanup_old_entries(self.sentiment_news_state_1d, 1440)
#                     self._cleanup_old_entries(self.sentiment_news_state_3d, 4320)

#             elif "alias" in data and "value" in data:
#                 series = data["alias"]
#                 alias = macro_alias.get(series)
#                 if alias:
#                     self.macro_state.put(alias, float(data["value"]))

#             if self.last_timer is None:
#                 next_ts = ctx.timer_service().current_processing_time() + 1000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer = next_ts

#         except Exception as e:
#             print(f"[ERROR] process_element exception: {e}", file=sys.stderr)

#     def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
#         try:
#             now = datetime.now(timezone.utc)

#             ts_str = now.isoformat()
#             key = ctx.get_current_key()

#             def safe_mean(values):
#                 return float(np.mean(values)) if values else None

#             def safe_mean_sentiment(values):
#                 return float(np.mean(values)) if values else 0.0

#             def safe_std(values):
#                 return float(np.std(values)) if values else None

#             def safe_sum(values):
#                 return float(np.sum(values)) if values else 0.0

#             features = {
#                 "ticker": key,
#                 "timestamp": ts_str,
#                 "price_mean_1min": safe_mean(self.price_state_1min.values()),
#                 "price_mean_5min": safe_mean(self.price_state_5min.values()),
#                 "price_std_5min": safe_std(self.price_state_5min.values()),
#                 "price_mean_30min": safe_mean(self.price_state_30min.values()),
#                 "price_std_30min": safe_std(self.price_state_30min.values()),
#                 "size_tot_1min": safe_sum(self.size_state_1min.values()),
#                 "size_tot_5min": safe_sum(self.size_state_5min.values()),
#                 "size_tot_30min": safe_sum(self.size_state_30min.values()),
#                 "sentiment_bluesky_mean_2hours": safe_mean_sentiment(self.sentiment_bluesky_state_2h.values()),
#                 "sentiment_bluesky_mean_1day": safe_mean_sentiment(self.sentiment_bluesky_state_1d.values()),
#                 "sentiment_reddit_mean_2hours": safe_mean_sentiment(self.sentiment_reddit_state_2h.values()),
#                 "sentiment_reddit_mean_1day": safe_mean_sentiment(self.sentiment_reddit_state_1d.values()),
#                 "sentiment_news_mean_1day": safe_mean_sentiment(self.sentiment_news_state_1d.values()),
#                 "sentiment_news_mean_3days": safe_mean_sentiment(self.sentiment_news_state_3d.values()),
#                 "sentiment_general_bluesky_mean_2hours": safe_mean_sentiment(self.sentiment_general_bluesky_state_2h.values()),
#                 "sentiment_general_bluesky_mean_1day": safe_mean_sentiment(self.sentiment_general_bluesky_state_1d.values()),
#                 "sentiment_general_reddit_mean_2hours": safe_mean_sentiment(self.sentiment_general_reddit_state_2h.values()),
#                 "sentiment_general_reddit_mean_1day": safe_mean_sentiment(self.sentiment_general_reddit_state_1d.values()),
#                 "minutes_since_open": (now - now.replace(hour=13, minute=30, second=0, microsecond=0)).total_seconds() // 60,
#                 "day_of_week": now.weekday(),
#                 "day_of_month": now.day,
#                 "week_of_year": now.isocalendar()[1],
#                 "month_of_year": now.month,
#                 "market_open_spike_flag": int(dtime(13, 30) <= now.time() < dtime(14, 0)),
#                 "market_close_spike_flag": int(dtime(19, 55) <= now.time() < dtime(20, 0)),
#             }

#             for k in macro_alias.values():
#                 features[k] = self.macro_state.get(k) if self.macro_state.contains(k) else None

#             output = json.dumps(features)
#             print(f"[OUTPUT] Emesso aggregato per ticker '{key}' alle {ts_str} → {output}", file=sys.stderr)
#             ctx.output(output)

#             next_ts = ctx.timer_service().current_processing_time() + 5000
#             ctx.timer_service().register_processing_time_timer(next_ts)
#             self.last_timer = next_ts

#         except Exception as e:
#             print(f"[ERROR] on_timer exception: {e}", file=sys.stderr)

# # === Instradamento per ticker ===
# def route_by_ticker(json_str):
#     try:
#         obj = json.loads(json_str)
#         return obj.get("ticker", "unknown")
#     except Exception as e:
#         print(f"[ERROR] route_by_ticker(): {e}", file=sys.stderr)
#         return "unknown"

# # === Avvio del job Flink ===
# if __name__ == "__main__":
#     print("[INFO] Starting Flink Preprocessing Job", file=sys.stderr)
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#     env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

#     env.enable_checkpointing(10000)

#     kafka_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink-preprocessor',
#         'auto.offset.reset': 'earliest'
#     }

#     consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "macrodata", "bluesky_sentiment", "reddit_sentiment", "news_sentiment"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )

#     producer = FlinkKafkaProducer(
#         topic="aggregated_data",
#         serialization_schema=SimpleStringSchema(),
#         producer_config={"bootstrap.servers": "kafka:9092"}
#     )

#     env.add_source(consumer) \
#         .key_by(route_by_ticker) \
#         .process(Aggregator(), output_type=Types.STRING()) \
#         .add_sink(producer)

#     env.execute("Unified Flink Preprocessor")
