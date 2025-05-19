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
import holidays
from pandas import Timestamp
from holidays import US


# === U.S. Market Holidays (semplificato) ===
# Lista dinamica di tutte le U.S. holidays per gli anni desiderati
us_holidays = holidays.US(years=range(2025, 2027))

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
        # Inizializzazione di tutti gli stati interni per ogni chiave (ticker)
        print("[DEBUG] open() called: initializing state", file=sys.stderr)
        self.price_state = ctx.get_map_state(MapStateDescriptor("price_state", Types.STRING(), Types.FLOAT()))
        self.size_state = ctx.get_map_state(MapStateDescriptor("size_state", Types.STRING(), Types.FLOAT()))
        self.sentiment_state = ctx.get_map_state(MapStateDescriptor("sentiment_state", Types.STRING(), Types.FLOAT()))
        self.general_sentiment_state = ctx.get_map_state(MapStateDescriptor("general_sentiment_state", Types.STRING(), Types.FLOAT()))
        self.macro_state = ctx.get_map_state(MapStateDescriptor("macro_state", Types.STRING(), Types.FLOAT()))
        # Liste con le ultime 20 osservazioni di prezzo e size → utili per rolling stats
        self.last_20_prices = ctx.get_list_state(ListStateDescriptor("last_20_prices", Types.FLOAT()))
        self.last_20_sizes = ctx.get_list_state(ListStateDescriptor("last_20_sizes", Types.FLOAT()))
        self.last_timer = None

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # Viene chiamato per ogni evento Kafka in input
        print(f"[DEBUG] process_element() received: {value}", file=sys.stderr)
        try:
            data = json.loads(value)
            now = datetime.fromisoformat(data["timestamp"])
            ts_str = data["timestamp"]
            key = ctx.get_current_key()   # il ticker (es: "AAPL")

            # === Caso 1: messaggio contiene prezzo e size ===
            if "price" in data:
                price = float(data["price"])
                size = float(data["size"])

                # Memorizziamo prezzo e size con timestamp
                self.price_state.put(ts_str, price)
                self.size_state.put(ts_str, size)

                 # Manteniamo una lista delle ultime 20 osservazioni
                prices = list(self.last_20_prices.get())
                sizes = list(self.last_20_sizes.get())
                prices.append(price)
                sizes.append(size)

                if len(prices) > 20: # Limitiamo a 20 elementi
                    prices = prices[-20:]
                    sizes = sizes[-20:]
                
                # Aggiorniamo lo stato con le liste troncate
                self.last_20_prices.update(prices)
                self.last_20_sizes.update(sizes)
                print(f"[DEBUG] updated price/size state for {key}", file=sys.stderr)

            # === Caso 2: messaggio contiene dato di sentiment ===
            elif "sentiment" in data:
                source = data.get("source")  # es: "bluesky" o "reddit"
                sentiment_key = f"{ts_str}:{source}"
                sentiment_value = float(data["sentiment"])

                # Se ticker == "general", salviamo in uno stato separato
                if data.get("ticker") == "general":
                    self.general_sentiment_state.put(sentiment_key, sentiment_value)
                else:
                    self.sentiment_state.put(sentiment_key, sentiment_value)
                print(f"[DEBUG] updated sentiment state for {key}", file=sys.stderr)
            
            # === Caso 3: messaggio contiene un dato macroeconomico ===
            elif "series_id" in data and "value" in data:
                series = data["series_id"]
                alias = macro_alias.get(series)
                if alias:
                    self.macro_state.put(alias, float(data["value"]))
                    print(f"[DEBUG] updated macro state: {alias} = {data['value']}", file=sys.stderr)

            # Registriamo il primo timer per innescare il calcolo periodico (ogni 5s)
            if self.last_timer is None:
                next_ts = ctx.timer_service().current_processing_time() + 1000
                ctx.timer_service().register_processing_time_timer(next_ts)
                self.last_timer = next_ts
                print(f"[DEBUG] registered first timer at {next_ts}", file=sys.stderr)

        except Exception as e:
            print(f"[ERROR] process_element exception: {e}", file=sys.stderr)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
         # Questa funzione viene eseguita ogni 5 secondi (timer)
        print(f"[DEBUG] on_timer() triggered at {timestamp}", file=sys.stderr)
        try:
            now = datetime.utcnow()
            ts_str = now.isoformat()
            key = ctx.get_current_key()
            print(f"[DEBUG] Timer triggered for key: {key} at {ts_str}", file=sys.stderr)

            # === Estrai le ultime osservazioni
            prices = list(self.last_20_prices.get())
            sizes = list(self.last_20_sizes.get())

            # if len(prices) < 6 or len(sizes) < 2:
            #     print(f"[DEBUG] Not enough data to compute features for {key}", file=sys.stderr)
            #     return

            # === Stock-derived features
            price_mean_5min = float(np.mean(prices[-5:]))
            log_return_5min = float(np.log(prices[-1] / prices[-6]))
            price_std_15min = float(np.std(prices[-15:])) if len(prices) >= 15 else np.nan
            price_max_30min = float(np.max(prices))

            size_1min = sizes[-1]
            size_diff_1min = sizes[-1] - sizes[-2]
            size_mean_15min = float(np.mean(sizes[-15:])) if len(sizes) >= 15 else np.nan
            size_std_15min = float(np.std(sizes[-15:])) if len(sizes) >= 15 else np.nan

            
            # === Sentiment features
            sentiments = [v for k, v in self.sentiment_state.items()]
            general_sentiments = [v for k, v in self.general_sentiment_state.items()]
            sentiment_mean_2h = float(np.mean(sentiments)) if sentiments else np.nan
            sentiment_mean_1d = float(np.mean(sentiments + general_sentiments)) if sentiments or general_sentiments else np.nan
            sentiment_delta_2h_1d = sentiment_mean_2h - sentiment_mean_1d if not np.isnan(sentiment_mean_2h) and not np.isnan(sentiment_mean_1d) else np.nan

            # === Macro variables
            macro = {k: v for k, v in self.macro_state.items()}

            # === Temporal features
            minutes_since_open = (now - now.replace(hour=9, minute=30, second=0, microsecond=0)).total_seconds() // 60
            sin_minute = float(np.sin(2 * np.pi * minutes_since_open / 390))
            cos_minute = float(np.cos(2 * np.pi * minutes_since_open / 390))

            holiday_proximity = min([abs((now.date() - h).days) for h in us_holidays if abs((now.date() - h).days) <= 5] + [5])


            # === Assemble feature dictionary
            features = {
                "ticker": key,
                "timestamp": ts_str,
                "price_mean_5min": price_mean_5min,
                "log_return_5min": log_return_5min,
                "price_std_15min": price_std_15min,
                "price_max_30min": price_max_30min,
                "Size_1min": size_1min,
                "Size_diff_1min": size_diff_1min,
                "Size_mean_15min": size_mean_15min,
                "Size_std_15min": size_std_15min,
                "sentiment_mean_2hours": sentiment_mean_2h,
                "sentiment_mean_1day": sentiment_mean_1d,
                "sentiment_delta_2hour_1day": sentiment_delta_2h_1d,
                "minutes_since_open": minutes_since_open,
                "sin_minute": sin_minute,
                "cos_minute": cos_minute,
                "holiday_proximity": holiday_proximity,
                "day_of_week": now.weekday(),
                "day_of_month": now.day,
                "week_of_year": now.isocalendar()[1],
                "month_of_year": now.month,
                "is_month_end": int(Timestamp(now).is_month_end),
                "is_quarter_end": int(Timestamp(now).is_quarter_end),
                "market_open_spike_flag": int(dtime(9, 30) <= now.time() < dtime(9, 35)),
                "market_close_spike_flag": int(dtime(15, 55) <= now.time() < dtime(16, 0)),
            }
            # Aggiungi le macro features (es: GDP, CPI...)
            features.update(macro)

            # Serializza in JSON e invia
            output = json.dumps(features)
            ctx.output(output)

            print(f"[INFO] Feature vector emitted for {key} at {ts_str}", file=sys.stderr)

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
