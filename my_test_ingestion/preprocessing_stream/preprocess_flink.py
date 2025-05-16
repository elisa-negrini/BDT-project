# preprocess_flink.py

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor, ListStateDescriptor
from datetime import datetime, timedelta, time as dtime, date
import json
import math
import numpy as np

# === Setup Flink environment ===
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

# === Kafka Configuration ===
kafka_props = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'flink-preprocessor'
}

# === Input Topics ===
consumer = FlinkKafkaConsumer(
    topics=["stock-data", "sentiment-data", "macro-data"],
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props
)

# === Output Topic ===
producer = FlinkKafkaProducer(
    topic="aggregated-features",
    serialization_schema=SimpleStringSchema(),
    producer_config={"bootstrap.servers": "kafka:9092"}
)

# === U.S. Market Holidays (semplificato) ===
US_MARKET_HOLIDAYS = {
    "2025-01-01", "2025-07-04", "2025-12-25"
}

# === Macro alias map ===
macro_alias = {
    "GDP": "gdp_nominal",
    "GDPC1": "gdp_real",
    "A191RL1Q225SBEA": "gdp_qoq",
    "CPIAUCSL": "cpi",
    "CPILFESL": "cpi_core",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment",
    "UMCSENT": "umich_sentiment"
}

# === Define Process Function ===
class Aggregator(KeyedProcessFunction):

    def open(self, ctx: RuntimeContext):
        self.price_state = ctx.get_map_state(MapStateDescriptor("price_state", Types.STRING(), Types.FLOAT()))
        self.size_state = ctx.get_map_state(MapStateDescriptor("size_state", Types.STRING(), Types.FLOAT()))
        self.sentiment_state = ctx.get_map_state(MapStateDescriptor("sentiment_state", Types.STRING(), Types.FLOAT()))
        self.general_sentiment_state = ctx.get_map_state(MapStateDescriptor("general_sentiment_state", Types.STRING(), Types.FLOAT()))
        self.macro_state = ctx.get_map_state(MapStateDescriptor("macro_state", Types.STRING(), Types.FLOAT()))
        self.last_20_prices = ctx.get_list_state(ListStateDescriptor("last_20_prices", Types.FLOAT()))
        self.last_20_sizes = ctx.get_list_state(ListStateDescriptor("last_20_sizes", Types.FLOAT()))
        self.last_timer = None

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        try:
            data = json.loads(value)
            now = datetime.fromisoformat(data["timestamp"])
            ts_str = data["timestamp"]
            key = ctx.get_current_key()

            if "price" in data:
                price = float(data["price"])
                size = float(data["size"])
                self.price_state.put(ts_str, price)
                self.size_state.put(ts_str, size)

                prices = list(self.last_20_prices.get())
                sizes = list(self.last_20_sizes.get())
                prices.append(price)
                sizes.append(size)
                if len(prices) > 20:
                    prices = prices[-20:]
                    sizes = sizes[-20:]
                self.last_20_prices.update(prices)
                self.last_20_sizes.update(sizes)

            elif "sentiment" in data:
                source = data.get("source")
                sentiment_key = f"{ts_str}:{source}"
                sentiment_value = float(data["sentiment"])
                if data.get("ticker") == "general":
                    self.general_sentiment_state.put(sentiment_key, sentiment_value)
                else:
                    self.sentiment_state.put(sentiment_key, sentiment_value)

            elif "series_id" in data and "value" in data:
                series = data["series_id"]
                alias = macro_alias.get(series)
                if alias:
                    self.macro_state.put(alias, float(data["value"]))

            if self.last_timer is None:
                next_ts = ctx.timer_service().current_processing_time() + 1000
                ctx.timer_service().register_processing_time_timer(next_ts)
                self.last_timer = next_ts

        except Exception as e:
            print(f"Error processing element: {e}")

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        now = datetime.utcnow()
        ts_str = now.isoformat()

        cutoff_30m = now - timedelta(minutes=30)
        for t in list(self.price_state.keys()):
            if datetime.fromisoformat(t) < cutoff_30m:
                self.price_state.remove(t)
                self.size_state.remove(t)

        cutoff_3d = now - timedelta(days=3)
        for t in list(self.sentiment_state.keys()):
            t_actual = datetime.fromisoformat(t.split(":"))[0]
            if t_actual < cutoff_3d:
                self.sentiment_state.remove(t)
        for t in list(self.general_sentiment_state.keys()):
            t_actual = datetime.fromisoformat(t.split(":"))[0]
            if t_actual < cutoff_3d:
                self.general_sentiment_state.remove(t)

        def mean_sentiment(state, window):
            cutoff = now - window
            values = [v for k, v in state.items() if datetime.fromisoformat(k.split(":"))[0] >= cutoff]
            return float(np.mean(values)) if values else None

        def values_since(state, cutoff):
            return [v for k, v in state.items() if datetime.fromisoformat(k) >= cutoff]

        prices_10s = values_since(self.price_state, now - timedelta(seconds=10))
        prices_1m = values_since(self.price_state, now - timedelta(minutes=1))
        prices_5m = values_since(self.price_state, now - timedelta(minutes=5))
        sizes_15m = values_since(self.size_state, now - timedelta(minutes=15))

        market_open = now.replace(hour=13, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=20, minute=0, second=0, microsecond=0)
        market_open_flag = market_open <= now <= market_close

        minutes_since_open = max(0, int((now - market_open).total_seconds() // 60))
        sin_minute = math.sin(2 * math.pi * (minutes_since_open % 390) / 390)
        cos_minute = math.cos(2 * math.pi * (minutes_since_open % 390) / 390)
        day_of_week = now.weekday()
        day_of_month = now.day
        week_of_year = now.isocalendar().week
        month_of_year = now.month
        next_day = now.date() + timedelta(days=1)
        is_month_end = next_day.month != now.month
        is_quarter_end = now.month in [3, 6, 9, 12] and is_month_end
        market_open_spike_flag = market_open <= now < (market_open + timedelta(minutes=5))
        market_close_spike_flag = (market_close - timedelta(minutes=5)) <= now < market_close
        holiday_proximity = min([abs((now.date() - date.fromisoformat(h)).days) for h in US_MARKET_HOLIDAYS] + [99])

        prices_fallback = list(self.last_20_prices.get())
        sizes_fallback = list(self.last_20_sizes.get())

        feature_row = {
            "timestamp": ts_str,
            "ticker": ctx.get_current_key(),
            "price_mean_10s": float(np.mean(prices_10s)) if prices_10s else None,
            "price_mean_1min": float(np.mean(prices_1m)) if prices_1m else None,
            "price_mean_5min": float(np.mean(prices_5m)) if prices_5m else None,
            "Size_mean_15min": float(np.mean(sizes_15m)) if sizes_15m else None,
            "Size_std_15min": float(np.std(sizes_15m)) if sizes_15m else None,
            "market_open": int(market_open_flag),
            "mean_price_last_20_stock": float(np.mean(prices_fallback)) if prices_fallback else None,
            "sd_price_last_20_stock": float(np.std(prices_fallback)) if prices_fallback else None,
            "mean_size_last_20_stock": float(np.mean(sizes_fallback)) if sizes_fallback else None,
            "sd_size_last_20_stock": float(np.std(sizes_fallback)) if sizes_fallback else None,
            "sentiment_mean_2hours": mean_sentiment(self.sentiment_state, timedelta(hours=2)),
            "sentiment_mean_1day": mean_sentiment(self.sentiment_state, timedelta(days=1)),
            "sentiment_mean_2hours_general": mean_sentiment(self.general_sentiment_state, timedelta(hours=2)),
            "sentiment_mean_1day_general": mean_sentiment(self.general_sentiment_state, timedelta(days=1)),
            "minutes_since_open": minutes_since_open,
            "sin_minute": sin_minute,
            "cos_minute": cos_minute,
            "day_of_week": day_of_week,
            "day_of_month": day_of_month,
            "week_of_year": week_of_year,
            "month_of_year": month_of_year,
            "is_month_end": int(is_month_end),
            "is_quarter_end": int(is_quarter_end),
            "holiday_proximity": holiday_proximity,
            "market_open_spike_flag": int(market_open_spike_flag),
            "market_close_spike_flag": int(market_close_spike_flag)
        }

        for k in macro_alias.values():
            if self.macro_state.contains(k):
                feature_row[k] = self.macro_state.get(k)

        ctx.output(None, json.dumps(feature_row))

        next_timer = ctx.timer_service().current_processing_time() + 1000
        ctx.timer_service().register_processing_time_timer(next_timer)
        self.last_timer = next_timer

# === Main pipeline ===
def route_by_ticker(json_str):
    try:
        obj = json.loads(json_str)
        return obj.get("ticker", "unknown")
    except:
        return "unknown"

env.add_source(consumer) \
    .key_by(route_by_ticker) \
    .process(Aggregator(), output_type=Types.STRING()) \
    .add_sink(producer)

env.execute("Unified Flink Preprocessor")
