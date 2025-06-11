import os
import sys
import json
import numpy as np
from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse
import pytz
from kafka import KafkaProducer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

# ==== Global Data Configuration ====
macro_alias = {
    "GDPC1": "gdp_real",
    "CPIAUCSL": "cpi",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment"
}

# Fixed key for global data state, not dependent on tickers
GLOBAL_DATA_KEY = "global_context_key"
MACRO_DATA_SUBKEY = "macro_data"
GENERAL_SENTIMENT_SUBKEY = "general_sentiment"

NY_TZ = pytz.timezone('America/New_York')

class GlobalDataAggregator(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        """
        Initializes state descriptors for storing macroeconomic data and general sentiment.
        Also initializes a Kafka producer for emitting aggregated global data.
        """
        def descriptor(name):
            return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

        self.macro_data_state = runtime_context.get_state(
            ValueStateDescriptor("macro_data_values", Types.MAP(Types.STRING(), Types.FLOAT())))
        
        self.sentiment_bluesky_general_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_general_2h"))
        self.sentiment_bluesky_general_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_general_1d"))

        self.output_producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def _cleanup_old_entries(self, state, window_minutes):
        """
        Removes entries from the given state that are older than the specified window.
        This keeps the data within the defined time windows.
        """
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

    def _emit_global_context_data(self):
        """
        Aggregates current macroeconomic data and general sentiment, then emits a JSON message
        to the 'global_data' Kafka topic.
        """
        now_utc = datetime.now(timezone.utc)
        ts_str = now_utc.isoformat()

        def calculate_mean(vals):
            vals = list(vals)
            return float(np.mean(vals)) if vals else 0.0

        self._cleanup_old_entries(self.sentiment_bluesky_general_2h, 2 * 60)
        self._cleanup_old_entries(self.sentiment_bluesky_general_1d, 24 * 60)

        current_macro_data = self.macro_data_state.value()
        if current_macro_data is None:
            current_macro_data = {}

        mean_bluesky_2h = calculate_mean(self.sentiment_bluesky_general_2h.values())
        mean_bluesky_1d = calculate_mean(self.sentiment_bluesky_general_1d.values())

        # Flatten the output data
        output_data = {
            "timestamp": ts_str,
            **current_macro_data,
            "sentiment_general_bluesky_mean_2hours": mean_bluesky_2h,
            "sentiment_general_bluesky_mean_1day": mean_bluesky_1d
        }
        
        print(f"[GLOBAL-AGGREGATION-EMIT] {ts_str} => {json.dumps(output_data)}", file=sys.stderr)
        self.output_producer.send('global_data', output_data)
        self.output_producer.flush() 

    def process_element(self, value, ctx):
        """
        Processes each incoming JSON element. Updates state based on whether it's
        macroeconomic data or general sentiment data. Emits aggregated global data
        whenever a relevant update occurs.
        """
        try:
            data = json.loads(value)
            
            emission_needed = False

            # Handle Macro Data
            if "alias" in data:
                alias_key = data.get("alias")
                new_value = float(data.get("value"))
                
                current_macro_data = self.macro_data_state.value()
                if current_macro_data is None:
                    current_macro_data = {}
                
                if current_macro_data.get(alias_key) != new_value:
                    current_macro_data[alias_key] = new_value
                    self.macro_data_state.update(current_macro_data)
                    print(f"[MACRO-GLOBAL] Updated {alias_key}: {new_value}", file=sys.stderr)
                    emission_needed = True

            # Handle General Sentiment Data
            elif "ticker" in data and isinstance(data["ticker"], list) and "GENERAL" in data["ticker"]:
                sentiment_score = float(data.get("sentiment_score"))
                ts_str = data.get("timestamp")

                if not ts_str:
                    print(f"[ERROR] Missing timestamp in general sentiment data: {data}", file=sys.stderr)
                    return 
                
                self.sentiment_bluesky_general_2h.put(ts_str, sentiment_score)
                self.sentiment_bluesky_general_1d.put(ts_str, sentiment_score)
                print(f"[GENERAL-SENTIMENT-GLOBAL] Added sentiment {sentiment_score} at {ts_str}", file=sys.stderr)
                emission_needed = True 
            else:
                print(f"[WARN] Data not processed by GlobalDataAggregator logic: {value}", file=sys.stderr)
                return 
            
            if emission_needed:
                self._emit_global_context_data()

        except json.JSONDecodeError:
            print(f"[ERROR] Failed to decode JSON in global job process_element: {value}", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] process_element in global job: {e} for value: {value}", file=sys.stderr)
            
    def close(self):
        if self.output_producer:
            self.output_producer.close()

# ==== Helper for Keying Global Data ====
def route_global_data_by_type(json_str):
    """
    Determines the key for incoming data in the global job.
    Routes macroeconomic data and general sentiment data to a fixed key.
    """
    try:
        data = json.loads(json_str)
        if "alias" in data:
            return GLOBAL_DATA_KEY 
        elif "ticker" in data and "GENERAL" in data["ticker"]:
            return GLOBAL_DATA_KEY
        else:
            print(f"[DEBUG-ROUTE] Discarding data (no alias/not general sentiment): {json_str}", file=sys.stderr)
            return "discard_key" 

    except json.JSONDecodeError:
        print(f"[WARN] Failed to decode JSON for key_by in global job: {json_str}", file=sys.stderr)
        return "invalid_json_key"
    except Exception as e:
        print(f"[ERROR] route_global_data_by_type: {e} for {json_str}", file=sys.stderr)
        return "error_key"

# ==== Flink Job Execution Setup ====
def main():
    """
    Sets up and executes the Flink stream processing job for global data aggregation.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_global_data_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics=["macrodata", "bluesky_sentiment"], 
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    
    filtered_stream = stream.filter(lambda x: route_global_data_by_type(x) == GLOBAL_DATA_KEY)

    keyed_global = filtered_stream.key_by(route_global_data_by_type, key_type=Types.STRING())
    
    keyed_global.process(GlobalDataAggregator(), output_type=Types.STRING())
    
    env.execute("Secondary Job: Global Data Aggregation (On-Change)")

if __name__ == "__main__":
    main()