from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from datetime import datetime, timezone, timedelta
import json
import numpy as np
import sys

TOP_30_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
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

        self.last_timer_state = runtime_context.get_state(
            ValueStateDescriptor("last_timer", Types.LONG()))

        self.macro_state = runtime_context.get_map_state(
            MapStateDescriptor("macro_state", Types.STRING(), Types.FLOAT()))

    def _cleanup_old_entries(self, state, window_minutes):
        threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        for k in list(state.keys()):
            try:
                if datetime.fromisoformat(k) < threshold:
                    state.remove(k)
            except:
                state.remove(k)

    def process_element(self, value, ctx):
        try:
            data = json.loads(value)
            if "alias" in data and "value" in data:
                alias = macro_alias.get(data["alias"])
                if alias:
                    self.macro_state.put(alias, float(data["value"]))
                return

            ticker = data.get("ticker")
            ts_str = data.get("timestamp")
            price = float(data.get("price"))
            size = float(data.get("size"))

            for state in [self.price_1m, self.price_5m, self.price_30m]:
                state.put(ts_str, price)
            for state in [self.size_1m, self.size_5m, self.size_30m]:
                state.put(ts_str, size)

            self._cleanup_old_entries(self.price_1m, 1)
            self._cleanup_old_entries(self.price_5m, 5)
            self._cleanup_old_entries(self.price_30m, 30)
            self._cleanup_old_entries(self.size_1m, 1)
            self._cleanup_old_entries(self.size_5m, 5)
            self._cleanup_old_entries(self.size_30m, 30)

            last_timer = self.last_timer_state.value()
            if last_timer is None:
                next_ts = ctx.timer_service().current_processing_time() + 60000
                ctx.timer_service().register_processing_time_timer(next_ts)
                self.last_timer_state.update(next_ts)

        except Exception as e:
            print(f"[ERROR] process_element: {e}", file=sys.stderr)

    def on_timer(self, timestamp, ctx):
        try:
            now = datetime.now(timezone.utc)
            ts_str = now.isoformat()
            ticker = ctx.get_current_key()

            def mean(vals):
                vals = list(vals)
                return float(np.mean(vals)) if vals else None

            def std(vals):
                vals = list(vals)
                return float(np.std(vals)) if vals else None

            def total(vals):
                vals = list(vals)
                return float(np.sum(vals)) if vals else 0.0

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
            }

            for macro_key in macro_alias.values():
                features[macro_key] = self.macro_state.get(macro_key) if self.macro_state.contains(macro_key) else None

            result = json.dumps(features)
            print(f"\U0001f4e4 [SENT] {ts_str} - {ticker} => {result}", file=sys.stderr)

            next_ts = ctx.timer_service().current_processing_time() + 5000
            ctx.timer_service().register_processing_time_timer(next_ts)
            self.last_timer_state.update(next_ts)

            return [result]
        except Exception as e:
            print(f"[ERROR] on_timer: {e}", file=sys.stderr)
            return []

def route_by_ticker(json_str):
    try:
        return json.loads(json_str).get("ticker", "unknown")
    except:
        return "unknown"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_stock_group'
    }

    consumer = FlinkKafkaConsumer(
        topics=["stock_trades", "macrodata"],
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    producer = FlinkKafkaProducer(
        topic='aggregated-data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    keyed = stream.key_by(route_by_ticker, key_type=Types.STRING())
    processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
    processed.add_sink(producer)

    env.execute("Full Aggregation with Sliding Windows and Macrodata")

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
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

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
#             ticker = data.get("ticker")
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

#             result = json.dumps(features)
#             print(f"\U0001f4e4 [SENT] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             next_ts = ctx.timer_service().current_processing_time() + 5000
#             ctx.timer_service().register_processing_time_timer(next_ts)
#             self.last_timer_state.update(next_ts)

#             return [result]
#         except Exception as e:
#             print(f"[ERROR] on_timer: {e}", file=sys.stderr)
#             return []

# def route_by_ticker(json_str):
#     try:
#         return json.loads(json_str).get("ticker", "unknown")
#     except:
#         return "unknown"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_group'
#     }

#     consumer = FlinkKafkaConsumer(
#         topics='stock_trades',
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregated_data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed = stream.key_by(route_by_ticker, key_type=Types.STRING())
#     processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
#     processed.add_sink(producer)

#     env.execute("Full Aggregation with Sliding Windows")

# if __name__ == "__main__":
#     main()
















































































# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.datastream.state import MapStateDescriptor
# from datetime import datetime, timezone, timedelta, time as dtime
# import json
# import numpy as np
# import sys

# macro_alias = {
#     "GDPC1": "gdp_real",
#     "CPIAUCSL": "cpi",
#     "FEDFUNDS": "ffr",
#     "DGS10": "t10y",
#     "DGS2": "t2y",
#     "T10Y2Y": "spread_10y_2y",
#     "UNRATE": "unemployment"
# }

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# class Aggregator(KeyedProcessFunction):
#     def open(self, ctx: RuntimeContext):
#         self.last_timer = None

#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         self.price_state_1min = ctx.get_map_state(descriptor("price_state_1min"))
#         self.price_state_5min = ctx.get_map_state(descriptor("price_state_5min"))
#         self.price_state_30min = ctx.get_map_state(descriptor("price_state_30min"))
#         self.size_state_1min = ctx.get_map_state(descriptor("size_state_1min"))
#         self.size_state_5min = ctx.get_map_state(descriptor("size_state_5min"))
#         self.size_state_30min = ctx.get_map_state(descriptor("size_state_30min"))

#         self.sentiment_bluesky_state_2h = ctx.get_map_state(descriptor("sentiment_bluesky_state_2h"))
#         self.sentiment_bluesky_state_1d = ctx.get_map_state(descriptor("sentiment_bluesky_state_1d"))
#         self.sentiment_news_state_1d = ctx.get_map_state(descriptor("sentiment_news_state_1d"))
#         self.sentiment_news_state_3d = ctx.get_map_state(descriptor("sentiment_news_state_3d"))
#         self.sentiment_general_bluesky_state_2h = ctx.get_map_state(descriptor("sentiment_general_bluesky_state_2h"))
#         self.sentiment_general_bluesky_state_1d = ctx.get_map_state(descriptor("sentiment_general_bluesky_state_1d"))

#         self.macro_state = ctx.get_map_state(descriptor("macro_state"))

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
#             ts_str = data.get("timestamp", datetime.utcnow().isoformat())
#             now = datetime.fromisoformat(ts_str)
#             key = ctx.get_current_key()

#             if "alias" in data and "value" in data:
#                 alias = macro_alias.get(data["alias"])
#                 if alias:
#                     for ticker in TOP_30_TICKERS:
#                         ctx.output(json.dumps({
#                             "ticker": ticker,
#                             "macro_update": {alias: float(data["value"])},
#                             "timestamp": ts_str
#                         }))
#                         self.macro_state.put(alias, float(data["value"]))
#                 return

#             if not (dtime(13, 31) <= now.time() <= dtime(20, 0)):
#                 return

#             if data.get("ticker") == "GENERAL":
#                 for ticker in TOP_30_TICKERS:
#                     data_copy = data.copy()
#                     data_copy["ticker"] = ticker
#                     self._handle_data(data_copy, ts_str)
#                     self._handle_general_sentiment(data_copy, ts_str)
#             else:
#                 self._handle_data(data, ts_str)

#             if self.last_timer is None:
#                 next_ts = ctx.timer_service().current_processing_time() + 1000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer = next_ts

#         except Exception as e:
#             print(f"[ERROR] process_element: {e}", file=sys.stderr)

#     def _handle_data(self, data, ts_str):
#         if "price" in data:
#             price = float(data["price"])
#             size = float(data["size"])

#             for state in [self.price_state_1min, self.price_state_5min, self.price_state_30min]:
#                 state.put(ts_str, price)

#             for state in [self.size_state_1min, self.size_state_5min, self.size_state_30min]:
#                 state.put(ts_str, size)

#             self._cleanup_price_size()

#         elif "sentiment" in data:
#             self._handle_sentiment(data, ts_str)

#     def _handle_general_sentiment(self, data, ts_str):
#         source = data.get("source")
#         sentiment_value = float(data["sentiment"])

#         def put_and_cleanup(state_2h, state_1d, m2=120, m1=1440):
#             state_2h.put(ts_str, sentiment_value)
#             state_1d.put(ts_str, sentiment_value)
#             self._cleanup_old_entries(state_2h, m2)
#             self._cleanup_old_entries(state_1d, m1)

#         if source == "bluesky":
#             put_and_cleanup(self.sentiment_general_bluesky_state_2h, self.sentiment_general_bluesky_state_1d)

#     def _handle_sentiment(self, data, ts_str):
#         source = data.get("source")
#         sentiment_value = float(data["sentiment"])

#         def put_and_cleanup(state_2h, state_1d, m2=120, m1=1440):
#             state_2h.put(ts_str, sentiment_value)
#             state_1d.put(ts_str, sentiment_value)
#             self._cleanup_old_entries(state_2h, m2)
#             self._cleanup_old_entries(state_1d, m1)

#         if source == "bluesky":
#             put_and_cleanup(self.sentiment_bluesky_state_2h, self.sentiment_bluesky_state_1d)
#         elif source == "news":
#             self.sentiment_news_state_1d.put(ts_str, sentiment_value)
#             self.sentiment_news_state_3d.put(ts_str, sentiment_value)
#             self._cleanup_old_entries(self.sentiment_news_state_1d, 1440)
#             self._cleanup_old_entries(self.sentiment_news_state_3d, 4320)

#     def _cleanup_price_size(self):
#         for state, minutes in zip([
#             self.price_state_1min, self.price_state_5min, self.price_state_30min,
#             self.size_state_1min, self.size_state_5min, self.size_state_30min
#         ], [1, 5, 30, 1, 5, 30]):
#             self._cleanup_old_entries(state, minutes)

# def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext', out):
#     try:
#         now = datetime.now(timezone.utc)
#         ts_str = now.isoformat()
#         key = ctx.get_current_key()

#         def mean(vals):
#             vals = list(vals)
#             return float(np.mean(vals)) if vals else None

#         def std(vals):
#             vals = list(vals)
#             return float(np.std(vals)) if vals else None

#         def total(vals):
#             vals = list(vals)
#             return float(np.sum(vals)) if vals else 0.0

#         def mean_sent(vals):
#             vals = list(vals)
#             return float(np.mean(vals)) if vals else 0.0

#         features = {
#             "ticker": key,
#             "timestamp": ts_str,
#             "price_mean_1min": mean(self.price_state_1min.values()),
#             "price_mean_5min": mean(self.price_state_5min.values()),
#             "price_std_5min": std(self.price_state_5min.values()),
#             "price_mean_30min": mean(self.price_state_30min.values()),
#             "price_std_30min": std(self.price_state_30min.values()),
#             "size_tot_1min": total(self.size_state_1min.values()),
#             "size_tot_5min": total(self.size_state_5min.values()),
#             "size_tot_30min": total(self.size_state_30min.values()),
#             "sentiment_bluesky_mean_2hours": mean_sent(self.sentiment_bluesky_state_2h.values()),
#             "sentiment_bluesky_mean_1day": mean_sent(self.sentiment_bluesky_state_1d.values()),
#             # "sentiment_reddit_mean_2hours": mean_sent(self.sentiment_reddit_state_2h.values()),
#             # "sentiment_reddit_mean_1day": mean_sent(self.sentiment_reddit_state_1d.values()),
#             "sentiment_news_mean_1day": mean_sent(self.sentiment_news_state_1d.values()),
#             "sentiment_news_mean_3days": mean_sent(self.sentiment_news_state_3d.values()),
#             "sentiment_general_bluesky_mean_2hours": mean_sent(self.sentiment_general_bluesky_state_2h.values()),
#             "sentiment_general_bluesky_mean_1day": mean_sent(self.sentiment_general_bluesky_state_1d.values()),
#             # "sentiment_general_reddit_mean_2hours": mean_sent(self.sentiment_general_reddit_state_2h.values()),
#             # "sentiment_general_reddit_mean_1day": mean_sent(self.sentiment_general_reddit_state_1d.values()),
#             "minutes_since_open": (now - now.replace(hour=13, minute=30, second=0, microsecond=0)).total_seconds() // 60,
#             "day_of_week": now.weekday(),
#             "day_of_month": now.day,
#             "week_of_year": now.isocalendar()[1],
#             "month_of_year": now.month,
#             "market_open_spike_flag": int(dtime(13, 30) <= now.time() < dtime(14, 0)),
#             "market_close_spike_flag": int(dtime(19, 55) <= now.time() < dtime(20, 0)),
#         }

#         for k in macro_alias.values():
#             features[k] = self.macro_state.get(k) if self.macro_state.contains(k) else None

#         out.collect(json.dumps(features))
#         print(f"📤 [SENT] {features['timestamp']} - {features['ticker']}", file=sys.stderr)

#         next_ts = ctx.timer_service().current_processing_time() + 5000
#         ctx.timer_service().register_processing_time_timer(next_ts)
#         self.last_timer = next_ts

#     except Exception as e:
#         print(f"[ERROR] on_timer: {e}", file=sys.stderr)

#     return []

# def route_by_ticker(json_str):
#     try:
#         data = json.loads(json_str)
#         return data.get("ticker", "GENERAL")
#     except:
#         return "GENERAL"

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "macrodata", "bluesky_sentiment", "news_sentiment"],
#         deserialization_schema=SimpleStringSchema(),
#         properties={
#             'bootstrap.servers': 'kafka:9092',
#             'group.id': 'flink_stock_group'
#         }
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregation',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
#     keyed_stream = stream.key_by(route_by_ticker, key_type=Types.STRING())
#     processed_stream = keyed_stream.process(Aggregator(), output_type=Types.STRING())
#     processed_stream.add_sink(producer)

#     env.execute("Stock Trade Processing Pipeline with Preprocessing")

# if __name__ == "__main__":
#     main()
