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
import sys

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
        print("[DEBUG] open() called: initializing state", file=sys.stderr)
        self.price_state = ctx.get_map_state(MapStateDescriptor("price_state", Types.STRING(), Types.FLOAT()))
        self.size_state = ctx.get_map_state(MapStateDescriptor("size_state", Types.STRING(), Types.FLOAT()))
        self.sentiment_state = ctx.get_map_state(MapStateDescriptor("sentiment_state", Types.STRING(), Types.FLOAT()))
        self.general_sentiment_state = ctx.get_map_state(MapStateDescriptor("general_sentiment_state", Types.STRING(), Types.FLOAT()))
        self.macro_state = ctx.get_map_state(MapStateDescriptor("macro_state", Types.STRING(), Types.FLOAT()))
        self.last_20_prices = ctx.get_list_state(ListStateDescriptor("last_20_prices", Types.FLOAT()))
        self.last_20_sizes = ctx.get_list_state(ListStateDescriptor("last_20_sizes", Types.FLOAT()))
        self.last_timer = None

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        print(f"[DEBUG] process_element() received: {value}", file=sys.stderr)
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
                print(f"[DEBUG] updated price/size state for {key}", file=sys.stderr)

            elif "sentiment" in data:
                source = data.get("source")
                sentiment_key = f"{ts_str}:{source}"
                sentiment_value = float(data["sentiment"])
                if data.get("ticker") == "general":
                    self.general_sentiment_state.put(sentiment_key, sentiment_value)
                else:
                    self.sentiment_state.put(sentiment_key, sentiment_value)
                print(f"[DEBUG] updated sentiment state for {key}", file=sys.stderr)

            elif "series_id" in data and "value" in data:
                series = data["series_id"]
                alias = macro_alias.get(series)
                if alias:
                    self.macro_state.put(alias, float(data["value"]))
                    print(f"[DEBUG] updated macro state: {alias} = {data['value']}", file=sys.stderr)

            if self.last_timer is None:
                next_ts = ctx.timer_service().current_processing_time() + 1000
                ctx.timer_service().register_processing_time_timer(next_ts)
                self.last_timer = next_ts
                print(f"[DEBUG] registered first timer at {next_ts}", file=sys.stderr)

        except Exception as e:
            print(f"[ERROR] process_element exception: {e}", file=sys.stderr)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        print(f"[DEBUG] on_timer() triggered at {timestamp}", file=sys.stderr)
        try:
            now = datetime.utcnow()
            ts_str = now.isoformat()
            key = ctx.get_current_key()
            print(f"[DEBUG] Timer triggered for key: {key} at {ts_str}", file=sys.stderr)

            # Debugging state sizes
            print(f"[DEBUG] price_state size: {len(list(self.price_state.items()))}", file=sys.stderr)
            print(f"[DEBUG] size_state size: {len(list(self.size_state.items()))}", file=sys.stderr)
            print(f"[DEBUG] sentiment_state size: {len(list(self.sentiment_state.items()))}", file=sys.stderr)
            print(f"[DEBUG] general_sentiment_state size: {len(list(self.general_sentiment_state.items()))}", file=sys.stderr)
            print(f"[DEBUG] macro_state size: {len(list(self.macro_state.items()))}", file=sys.stderr)

            # Re-register the timer to keep it running every 5 seconds
            next_ts = ctx.timer_service().current_processing_time() + 5000
            ctx.timer_service().register_processing_time_timer(next_ts)
            self.last_timer = next_ts
            print(f"[DEBUG] Next timer scheduled at {next_ts}", file=sys.stderr)

        except Exception as e:
            print(f"[ERROR] on_timer exception: {e}", file=sys.stderr)


# === Main pipeline ===
def route_by_ticker(json_str):
    try:
        obj = json.loads(json_str)
        return obj.get("ticker", "unknown")
    except Exception as e:
        print(f"[ERROR] route_by_ticker(): {e}", file=sys.stderr)
        return "unknown"

if __name__ == "__main__":
    print("[INFO] Starting Flink Preprocessing Job", file=sys.stderr)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    # ✅ Abilita checkpoint ogni 10 secondi
    env.enable_checkpointing(10000)

    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-preprocessor',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics=["stock_trades", "macrodata"], # "bluesky_sentiment", 
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    producer = FlinkKafkaProducer(
        topic="aggregated_data",
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": "kafka:9092"}
    )

    env.add_source(consumer) \
        .key_by(route_by_ticker) \
        .process(Aggregator(), output_type=Types.STRING()) \
        .add_sink(producer)

    env.execute("Unified Flink Preprocessor")
