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
#     def open(self, ctx: RuntimeContext):
#         def descriptor(name): return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())
#         self.price_1m = ctx.get_map_state(descriptor("price_1m"))
#         self.price_5m = ctx.get_map_state(descriptor("price_5m"))
#         self.price_30m = ctx.get_map_state(descriptor("price_30m"))
#         self.size_1m = ctx.get_map_state(descriptor("size_1m"))
#         self.size_5m = ctx.get_map_state(descriptor("size_5m"))
#         self.size_30m = ctx.get_map_state(descriptor("size_30m"))

#         self.sentiment_bluesky_2h = ctx.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = ctx.get_map_state(descriptor("sentiment_bluesky_1d"))
#         self.sentiment_news_1d = ctx.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = ctx.get_map_state(descriptor("sentiment_news_3d"))
#         self.sentiment_general_bluesky_2h = ctx.get_map_state(descriptor("sentiment_general_bluesky_2h"))
#         self.sentiment_general_bluesky_1d = ctx.get_map_state(descriptor("sentiment_general_bluesky_1d"))
#         # self.sentiment_reddit_2h = ctx.get_map_state(descriptor("sentiment_reddit_2h"))
#         # self.sentiment_reddit_1d = ctx.get_map_state(descriptor("sentiment_reddit_1d"))
#         # self.sentiment_general_reddit_2h = ctx.get_map_state(descriptor("sentiment_general_reddit_2h"))
#         # self.sentiment_general_reddit_1d = ctx.get_map_state(descriptor("sentiment_general_reddit_1d"))

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

#             if "sentiment" in data:
#                 source = data.get("source")
#                 sentiment = float(data["sentiment"])
#                 ts_str = data["timestamp"]
#                 is_general = data.get("ticker") == "GENERAL"
#                 tickers = TOP_30_TICKERS if is_general else [data.get("ticker")]

#                 for ticker in tickers:
#                     ctx.set_current_key(ticker)
#                     if source == "bluesky":
#                         self.sentiment_bluesky_2h.put(ts_str, sentiment)
#                         self.sentiment_bluesky_1d.put(ts_str, sentiment)
#                         if is_general:
#                             self.sentiment_general_bluesky_2h.put(ts_str, sentiment)
#                             self.sentiment_general_bluesky_1d.put(ts_str, sentiment)
#                     elif source == "news":
#                         self.sentiment_news_1d.put(ts_str, sentiment)
#                         self.sentiment_news_3d.put(ts_str, sentiment)
#                     # elif source == "reddit":
#                     #     self.sentiment_reddit_2h.put(ts_str, sentiment)
#                     #     self.sentiment_reddit_1d.put(ts_str, sentiment)
#                     #     if is_general:
#                     #         self.sentiment_general_reddit_2h.put(ts_str, sentiment)
#                     #         self.sentiment_general_reddit_1d.put(ts_str, sentiment)

#                 return

#             ticker = data.get("ticker")
#             if ticker not in TOP_30_TICKERS:
#                 return

#             ts_str = data["timestamp"]
#             price = float(data["price"])
#             size = float(data["size"])
#             ctx.set_current_key(ticker)

#             for state in [self.price_1m, self.price_5m, self.price_30m]:
#                 state.put(ts_str, price)
#             for state in [self.size_1m, self.size_5m, self.size_30m]:
#                 state.put(ts_str, size)

#             for s, m in [
#                 (self.price_1m, 1), (self.price_5m, 5), (self.price_30m, 30),
#                 (self.size_1m, 1), (self.size_5m, 5), (self.size_30m, 30),
#                 (self.sentiment_bluesky_2h, 120), (self.sentiment_bluesky_1d, 1440),
#                 (self.sentiment_news_1d, 1440), (self.sentiment_news_3d, 4320),
#                 (self.sentiment_general_bluesky_2h, 120), (self.sentiment_general_bluesky_1d, 1440)
#             ]:
#                 self._cleanup_old_entries(s, m)

#             if self.last_timer_state.value() is None:
#                 next_ts = ctx.timer_service().current_processing_time() + 5000
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
#                 "sentiment_bluesky_mean_2hours": mean(self.sentiment_bluesky_2h.values()),
#                 "sentiment_bluesky_mean_1day": mean(self.sentiment_bluesky_1d.values()),
#                 "sentiment_news_mean_1day": mean(self.sentiment_news_1d.values()),
#                 "sentiment_news_mean_3days": mean(self.sentiment_news_3d.values()),
#                 "sentiment_general_bluesky_mean_2hours": mean(self.sentiment_general_bluesky_2h.values()),
#                 "sentiment_general_bluesky_mean_1day": mean(self.sentiment_general_bluesky_1d.values()),
#                 # "sentiment_reddit_mean_2hours": mean(self.sentiment_reddit_2h.values()),
#                 # "sentiment_reddit_mean_1day": mean(self.sentiment_reddit_1d.values()),
#                 # "sentiment_general_reddit_mean_2hours": mean(self.sentiment_general_reddit_2h.values()),
#                 # "sentiment_general_reddit_mean_1day": mean(self.sentiment_general_reddit_1d.values()),
#             }

#             for macro_key in macro_alias.values():
#                 features[macro_key] = macro_data_dict.get(macro_key)

#             result = json.dumps(features)
#             print(f"📤 [SENT] {ts_str} - {ticker} => {result}", file=sys.stderr)

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

#     consumer_sentiment = FlinkKafkaConsumer(
#         topics=["bluesky_sentiment", "news_sentiment"],
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
#     stream_sentiment = env.add_source(consumer_sentiment, type_info=Types.STRING())

#     merged_stream = stream_stock.union(stream_macro).union(stream_sentiment)
#     keyed = merged_stream.key_by(route_by_ticker, key_type=Types.STRING())
#     processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
#     processed.add_sink(producer)

#     env.execute("Full Aggregation with Sliding Windows and Macrodata")

# if __name__ == "__main__":
#     main()

















































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

macro_data_dict = {}

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
                alias_key = data.get("alias")
                if alias_key:
                    macro_data_dict[alias_key] = float(data["value"])
                    print(macro_data_dict)
                return

            ticker = data.get("ticker")
            if ticker not in TOP_30_TICKERS:
                return

            ctx.set_current_key(ticker)

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
                features[macro_key] = macro_data_dict.get(macro_key)

            result = json.dumps(features)
            print(f"📤 [SENT2] {ts_str} - {ticker} => {result}", file=sys.stderr)

            next_ts = ctx.timer_service().current_processing_time() + 5000
            ctx.timer_service().register_processing_time_timer(next_ts)
            self.last_timer_state.update(next_ts)

            return result
        except Exception as e:
            print(f"[ERROR] on_timer: {e}", file=sys.stderr)
            return json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat()})

def route_by_ticker(json_str):
    try:
        data = json.loads(json_str)
        return data.get("ticker") or "unknown"
    except:
        return "unknown"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_stock_group',
        'auto.offset.reset': 'earliest'
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

































