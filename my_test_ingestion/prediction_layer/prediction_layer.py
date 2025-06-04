

#FUNZIONA MA SENZA FLINK
import os
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import joblib
import json
import datetime
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

# Paths for saving artifacts (must be the same as in the training script)
MODEL_SAVE_PATH = "create_model_lstm/model"
MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, "lstm_model_AAPL.h5")
SCALER_FILENAME = os.path.join(MODEL_SAVE_PATH, "scaler_AAPL.pkl")
TICKER_MAP_FILENAME = os.path.join(MODEL_SAVE_PATH, "ticker_map.json")

# N_STEPS must match the N_STEPS used in training (e.g., 5 for 5 aggregated 10-second points)
N_STEPS = 5 # Crucial: this must be the same value as in training

# Number of offset-based buffers (every 10 seconds, from :00 to :50)
NUM_OFFSET_BUFFERS = 6 # Corresponds to offsets :00, :10, :20, :30, :40, :50

# --- Kafka Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = "aggregated_data" # This is the topic the Flink aggregator produces to
KAFKA_GROUP_ID = "prediction_consumer_group"

# --- Load Artifacts ---
try:
    model = tf.keras.models.load_model(MODEL_FILENAME)
    print(f"\u2705 Model loaded from {MODEL_FILENAME}")

    scaler = joblib.load(SCALER_FILENAME)
    print(f"\u2705 Scaler loaded from {SCALER_FILENAME}")

    with open(TICKER_MAP_FILENAME, 'r') as f:
        ticker_name_to_code_map = json.load(f)
    print(f"\u2705 Ticker mapping loaded from {TICKER_MAP_FILENAME}")

    # The number of features expected by the scaler MUST match the number of features
    # being sent by the Flink aggregator.
    num_features = scaler.n_features_in_
    print(f"Number of features expected by the scaler: {num_features}")

except Exception as e:
    print(f"\u274C Error loading artifacts: {e}")
    sys.exit(1)

### Data Handling and Real-time Prediction

# realtime_data_buffers will now contain a list of NUM_OFFSET_BUFFERS lists for each ticker.
# Example: {'AAPL': [buffer_offset_00, buffer_offset_10, ..., buffer_offset_50]}
realtime_data_buffers = {}
# last_timestamps remains a single timestamp per ticker for order control
last_timestamps = {}

def get_ticker_code(ticker_name):
    """Returns the numeric code for a ticker name."""
    return ticker_name_to_code_map.get(ticker_name)

def make_prediction(ticker_name, new_data_timestamp, new_data_features_dict):
    """
    Handles the arrival of new data and makes a prediction using the appropriate buffer.

    Args:
        ticker_name (str): The name of the ticker (e.g., 'AAPL').
        new_data_timestamp (datetime.datetime): The timestamp of the new aggregated data.
        new_data_features_dict (dict): The dictionary of aggregated features (NOT scaled) for this timestamp,
                                       as produced by the Flink aggregator.
    """
    global realtime_data_buffers, last_timestamps

    ticker_code = get_ticker_code(ticker_name)
    if ticker_code is None:
        print(f"\u274C Ticker '{ticker_name}' not found in mapping. Ignoring message.")
        return None

    # Initialize the NUM_OFFSET_BUFFERS buffers for the ticker if it's new
    if ticker_name not in realtime_data_buffers:
        realtime_data_buffers[ticker_name] = [[] for _ in range(NUM_OFFSET_BUFFERS)]
        last_timestamps[ticker_name] = None
        print(f"Initialized {NUM_OFFSET_BUFFERS} buffers for ticker: {ticker_name}")

    # Check if the timestamp is ahead (to avoid out-of-order or duplicate data)
    # This also handles the case where the Flink job might re-send messages on recovery,
    # though Flink's exactly-once semantics help with that at the Flink level.
    if last_timestamps[ticker_name] is not None and new_data_timestamp <= last_timestamps[ticker_name]:
        # print(f"Warning: Old or duplicate data for {ticker_name} at {new_data_timestamp}. Ignoring.")
        return None

    # --- Feature Extraction and Ordering ---
    # The order of features in this list MUST match the order used when the scaler was trained.
    # This list corresponds to the 'features' dictionary being sent by the Flink aggregator.
    # Ensure all possible features from Flink are accounted for.
    # If any feature is missing or None, handle it gracefully (e.g., default to 0 or mean imputation).
    # For simplicity, we'll assume the Flink aggregator always sends all fields.
    # If a feature might be missing, add a .get() with a default value.

    expected_feature_keys_in_order = [
        "price_mean_1min", "price_mean_5min", "price_std_5min", "price_mean_30min", "price_std_30min",
        "size_tot_1min", "size_tot_5min", "size_tot_30min",
        "sentiment_bluesky_mean_2h", "sentiment_bluesky_mean_1d",
        "sentiment_news_mean_1d", "sentiment_news_mean_3d",
        "sentiment_bluesky_mean_general_2hours", "sentiment_bluesky_mean_general_1d",
        "minutes_since_open", "day_of_week", "day_of_month", "week_of_year", "month_of_year",
        "market_open_spike_flag", "market_close_spike_flag",
        "eps", "freeCashFlow", "profit_margin", "debt_to_equity",
        # Macro data features - these are dynamic, so we need to collect them
        # from the macro_alias used in the Flink script.
        "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"
    ]
    
    current_features_for_scaling = []
    for key in expected_feature_keys_in_order:
        value = new_data_features_dict.get(key)
        if value is None:
            # Handle missing fundamental or macro data by using a default (e.g., 0)
            # or a more sophisticated imputation strategy if known from training.
            # For now, print a warning and use 0.0.
            # The model's training data should reflect how these missing values are handled.
            print(f"\u274C Warning: Missing feature '{key}' for {ticker_name} at {new_data_timestamp}. Using 0.0.", file=sys.stderr)
            current_features_for_scaling.append(0.0)
        else:
            current_features_for_scaling.append(float(value)) # Ensure float type

    new_data_features_np = np.array(current_features_for_scaling, dtype=np.float32)

    # Verify the number of features matches what the scaler expects
    if new_data_features_np.shape[0] != num_features:
        print(f"\u274C Feature mismatch for {ticker_name} at {new_data_timestamp}: "
              f"Expected {num_features} features, got {new_data_features_np.shape[0]}. "
              f"Please ensure the Flink aggregator output and the scaler's training data match feature sets and order.")
        return None

    # Apply the scaler to the new feature point
    try:
        scaled_features = scaler.transform(new_data_features_np.reshape(1, -1)).flatten()
    except ValueError as ve:
        print(f"\u274C Scaling error for {ticker_name} at {new_data_timestamp}: {ve}. Ensure features have the correct dimension ({num_features}).")
        return None
    except Exception as e:
        print(f"\u274C Unexpected error during scaling for {ticker_name} at {new_data_timestamp}: {e}")
        return None

    # Determine the buffer index based on the second of the timestamp
    # Flink aggregator guarantees seconds will be 00, 10, ..., 50
    second_of_minute = new_data_timestamp.second
    
    # Ensure the second is a multiple of 10 and within the valid range (0-50)
    if second_of_minute % 10 != 0 or second_of_minute >= NUM_OFFSET_BUFFERS * 10:
        print(f"\u274C Warning: Timestamp {new_data_timestamp} (second: {second_of_minute:02d}) is not aligned to an expected 10-second offset (00, 10, ..., 50). Ignoring the point.")
        return None
    
    buffer_index = second_of_minute // 10 # This correctly maps 00->0, 10->1, 20->2, etc.

    # Add the scaled features to the specific offset buffer
    current_ticker_buffers = realtime_data_buffers[ticker_name]
    target_buffer = current_ticker_buffers[buffer_index]
    
    target_buffer.append(scaled_features)

    # Remove the oldest data if the buffer exceeds N_STEPS
    if len(target_buffer) > N_STEPS:
        target_buffer.pop(0)

    last_timestamps[ticker_name] = new_data_timestamp

    # Check if the specific buffer (the one just updated) has enough data to predict
    if len(target_buffer) == N_STEPS:
        # Prepare input for the model
        # input_sequence must be (1, N_STEPS, num_features)
        input_sequence = np.array(target_buffer).reshape(1, N_STEPS, num_features)
        
        # Prepare ticker input (1,)
        input_ticker_code = np.array([ticker_code], dtype=np.int32)

        # Perform the prediction
        try:
            prediction = model.predict([input_sequence, input_ticker_code], verbose=0)[0][0]
            #prediction = model.predict(input_sequence, verbose=0)[0][0]
            # Also log the is_simulated_prediction flag from the Flink output
            is_simulated = new_data_features_dict.get("is_simulated_prediction", False)
            print(f"Prediction for {ticker_name} (offset :{second_of_minute:02d}) at {new_data_timestamp} (Simulated: {is_simulated}): {prediction:.4f}")
            return prediction
        except Exception as e:
            print(f"\u274C Error during prediction for {ticker_name} at {new_data_timestamp}: {e}")
            return None
    else:
        # print(f"Buffer for {ticker_name} (offset :{second_of_minute:02d}) has {len(target_buffer)} points, needs {N_STEPS} to predict.")
        return None

if __name__ == "__main__":
    print("\n\U0001F535 Starting Kafka consumer for real-time predictions...")

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(conf)

    try:
        consumer.subscribe([KAFKA_TOPIC])
        print(f"\u2705 Subscribed to Kafka topic: {KAFKA_TOPIC}")

        print("\nWaiting for messages from Kafka...")
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    ticker_name = data.get('ticker')
                    timestamp_str = data.get('timestamp')
                    # The 'features' are now a dictionary from the Flink aggregator
                    features_dict = {k: v for k, v in data.items() if k not in ['ticker', 'timestamp', 'is_simulated_prediction']}
                    # Also explicitly extract is_simulated_prediction if you want to pass it
                    is_simulated_prediction_flag = data.get('is_simulated_prediction', False)


                    if not all([ticker_name, timestamp_str, features_dict is not None]):
                        print(f"\u274C Malformed Kafka message: {data}. Expected 'ticker', 'timestamp', and aggregated features.")
                        continue

                    # Convert timestamp to datetime object
                    # Flink uses ISO format, so datetime.fromisoformat is appropriate
                    new_data_timestamp = datetime.datetime.fromisoformat(timestamp_str)
                    
                    # Pass the features_dict directly to make_prediction
                    make_prediction(ticker_name, new_data_timestamp, data) # Pass the whole dict to get all features including flag

                except json.JSONDecodeError:
                    print(f"\u274C JSON decoding error from Kafka message: {msg.value()}")
                except Exception as e:
                    print(f"\u274C Error processing Kafka message: {e} - Message: {msg.value()}")

    except KeyboardInterrupt:
        print("\n\u2705 Keyboard interrupt. Closing Kafka consumer.")
    finally:
        consumer.close()
        print("Kafka consumer closed.")




# import os
# import json
# import numpy as np
# import tensorflow as tf
# import pickle
# from datetime import datetime, timedelta
# import logging

# from pyflink.common import WatermarkStrategy, Duration, Row, RestartStrategies
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
# # ADD FlinkKafkaProducer HERE
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer # <--- Make sure FlinkKafkaProducer is in this line
# from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.checkpointing_mode import CheckpointingMode




# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# MODEL_PATH = "/opt/flink/model/lstm_multi_ticker.h5"
# SCALER_PATH = "/opt/flink/model/scaler.pkl"
# TICKER_MAP_PATH = "/opt/flink/model/ticker_map.json"

# FEATURE_COLUMNS_ORDERED = [
#     "price_mean_1min", "price_mean_5min", "price_cv_5min", "price_mean_30min", "price_cv_30min",
#     "size_tot_1min", "size_tot_5min", "size_tot_30min",
#     "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
#     "sentiment_news_mean_1day", "sentiment_news_mean_3days",
#     "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
#     "minutes_since_open", "day_of_week", "day_of_month", "week_of_year", "month_of_year",
#     "market_open_spike_flag", "market_close_spike_flag", "eps", "free_cash_flow",
#     "profit_margin", "debt_to_equity", "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"
# ]

# def safe_float(x):
#     try:
#         return float(x)
#     except Exception:
#         return 0.0

# class LSTMPredictionFunction(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         logger.info("Opening LSTMPredictionFunction.")

#         try:
#             assert os.path.exists(MODEL_PATH), f"Model file not found at {MODEL_PATH}"
#             self.model = tf.keras.models.load_model(MODEL_PATH)
#             logger.info(f"Model loaded from {MODEL_PATH}")
#         except Exception as e:
#             logger.error(f"Failed to load model: {e}")
#             raise e

#         try:
#             assert os.path.exists(SCALER_PATH), f"Scaler file not found at {SCALER_PATH}"
#             with open(SCALER_PATH, 'rb') as f:
#                 self.scaler = pickle.load(f)
#             logger.info(f"Scaler loaded from {SCALER_PATH}")
#         except Exception as e:
#             logger.error(f"Failed to load scaler: {e}")
#             raise e

#         try:
#             assert os.path.exists(TICKER_MAP_PATH), f"Ticker map not found at {TICKER_MAP_PATH}"
#             with open(TICKER_MAP_PATH, 'r') as f:
#                 self.ticker_map = json.load(f)
#             logger.info(f"Ticker map loaded from {TICKER_MAP_PATH}")
#         except Exception as e:
#             logger.error(f"Failed to load ticker map: {e}")
#             raise e

#         self.buffer_state = self.get_runtime_context().get_map_state(
#             "buffer_state",
#             Types.STRING(),
#             Types.MAP(Types.STRING(), Types.FLOAT())
#         )
#         self.timer_service = self.get_runtime_context().timer_service()
#         logger.info("LSTMPredictionFunction opened successfully.")

#     def process_element(self, value: Row, ctx: 'KeyedProcessFunction.Context'):
#         try:
#             ticker = value["ticker"]
#             timestamp_str = value["timestamp"]
#             timestamp_dt = datetime.fromisoformat(timestamp_str)

#             logger.debug(f"Processing element for ticker: {ticker}, timestamp: {timestamp_str}")

#             current_buffer_map = self.buffer_state.get(ticker) or {}
#             data_point_features = {
#                 col: safe_float(value.get(col, 0.0)) for col in FEATURE_COLUMNS_ORDERED
#             }

#             current_buffer_map[timestamp_str] = data_point_features
#             sorted_buffer_items = sorted(current_buffer_map.items(), key=lambda item: item[0], reverse=True)[:6]
#             self.buffer_state.put(ticker, dict(sorted_buffer_items))

#             if len(sorted_buffer_items) == 6:
#                 logger.info(f"Buffer full for ticker {ticker}. Performing prediction.")
#                 features_for_prediction = []

#                 for ts, dp in sorted(sorted_buffer_items, key=lambda item: item[0]):
#                     features_for_prediction.append([dp[col] for col in FEATURE_COLUMNS_ORDERED])

#                 features_np = np.array(features_for_prediction)
#                 logger.debug(f"Feature shape before scaling: {features_np.shape}")

#                 num_features = features_np.shape[1]
#                 scaled_2d = self.scaler.transform(features_np.reshape(-1, num_features))
#                 scaled_input = scaled_2d.reshape(1, 6, num_features)

#                 logger.debug(f"Scaled input shape for model: {scaled_input.shape}")
#                 prediction = self.model.predict(scaled_input)[0][0]
#                 logger.info(f"Prediction for {ticker} at {timestamp_str}: {prediction}")

#                 prediction_output = {
#                     "ticker": ticker,
#                     "timestamp_predicted": (timestamp_dt + timedelta(minutes=1)).isoformat(timespec='seconds'),
#                     "predicted_price": float(prediction)
#                 }

#                 yield Row(**prediction_output)
#                 self.buffer_state.put(ticker, {})
#                 logger.debug(f"Buffer reset for ticker: {ticker}")

#         except Exception as e:
#             logger.error(f"Error processing element: {e}")

#     def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
#         logger.debug(f"Timer triggered for {ctx.current_key()} at {datetime.fromtimestamp(timestamp / 1000.0)}")

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.enable_checkpointing(60000)
#     env.get_checkpoint_config().set_checkpoint_timeout(300000)
#     env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
#     env.set_parallelism(1)

#     KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
#     KAFKA_TOPIC_AGGREGATED = os.getenv("KAFKA_TOPIC_AGGREGATED", "aggregated_data")
#     KAFKA_TOPIC_DASHBOARD = os.getenv("KAFKA_TOPIC_DASHBOARD", "dashboard")

#     logger.info(f"Kafka Broker: {KAFKA_BROKER}")
#     logger.info(f"Kafka Input Topic: {KAFKA_TOPIC_AGGREGATED}")
#     logger.info(f"Kafka Output Topic: {KAFKA_TOPIC_DASHBOARD}")

#     input_column_names = ["ticker", "timestamp"] + FEATURE_COLUMNS_ORDERED
#     input_column_types = [Types.STRING(), Types.STRING()] + [Types.FLOAT()] * len(FEATURE_COLUMNS_ORDERED)

#     kafka_deserialization_schema = JsonRowDeserializationSchema.builder() \
#         .type_info(Types.ROW_NAMED(input_column_names, input_column_types)).build()

#     # --- CHANGES START HERE ---

#     # Configure the KafkaSource
#     kafka_source = KafkaSource.builder() \
#         .set_bootstrap_servers(KAFKA_BROKER) \
#         .set_topics(KAFKA_TOPIC_AGGREGATED) \
#         .set_group_id("lstm_prediction_group") \
#         .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
#         .set_value_only_deserializer(kafka_deserialization_schema) \
#         .build()

#     data_stream = env.from_source(
#         source=kafka_source,  # Pass the KafkaSource object
#         watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
#         source_name="Kafka Source"
#     )

#     # --- CHANGES END HERE ---

#     producer_serialization_schema = JsonRowSerializationSchema.builder() \
#         .with_type_info(
#             Types.ROW_NAMED(
#                 ["ticker", "timestamp_predicted", "predicted_price"],
#                 [Types.STRING(), Types.STRING(), Types.FLOAT()]
#             )
#         ).build()

#     # FlinkKafkaProducer is still fine for sinks
#     kafka_producer = FlinkKafkaProducer(
#         KAFKA_TOPIC_DASHBOARD,
#         producer_serialization_schema,
#         {'bootstrap.servers': KAFKA_BROKER},
#     )

#     predicted_stream = data_stream.key_by(lambda x: x["ticker"]).process(LSTMPredictionFunction())
#     predicted_stream.print()
#     predicted_stream.add_sink(kafka_producer)

#     logger.info("Starting Flink job...")
#     env.execute("LSTM Stock Price Prediction Job")

# if __name__ == "__main__":
#     main()





# import os
# import numpy as np
# import pandas as pd # Not strictly necessary for this refactor but good practice
# import tensorflow as tf
# from sklearn.preprocessing import MinMaxScaler
# import joblib
# import json
# import datetime
# import time
# import sys

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, udf, lit
# from pyspark.sql.types import StringType, StructType, StructField, DoubleType, TimestampType, BooleanType, IntegerType
# from pyspark.sql.streaming import StreamingQueryException

# # --- Paths for saving artifacts (must be the same as in the training script) ---
# MODEL_SAVE_PATH = "create_model_lstm/model"
# MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, "lstm_multi_ticker.h5")
# SCALER_FILENAME = os.path.join(MODEL_SAVE_PATH, "scaler.pkl")
# TICKER_MAP_FILENAME = os.path.join(MODEL_SAVE_PATH, "ticker_map.json")

# # N_STEPS must match the N_STEPS used in training (e.g., 5 for 5 aggregated 10-second points)
# N_STEPS = 5 # Crucial: this must be the same value as in training

# # Number of offset-based buffers (every 10 seconds, from :00 to :50)
# NUM_OFFSET_BUFFERS = 6 # Corresponds to offsets :00, :10, :20, :30, :40, :50

# # --- Kafka Configuration ---
# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092") # Use localhost for local testing
# KAFKA_TOPIC = "aggregated_data" # This is the topic the Flink aggregator produces to
# KAFKA_GROUP_ID = "spark_prediction_consumer_group" # Unique group ID for Spark

# # --- Spark Session Initialization ---
# # Spark uses Scala 2.12 by default for Spark 3.x, ensure Kafka package matches
# spark = SparkSession.builder \
#     .appName("RealTimeStockPrediction") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN") # Reduce verbosity of Spark logs

# print("\n\U0001F535 SparkSession initialized.")

# # --- Load Artifacts (Broadcast to all executors) ---
# # These will be loaded once by the driver and then distributed.
# try:
#     # Load model and make it non-trainable to optimize inference
#     loaded_model = tf.keras.models.load_model(MODEL_FILENAME)
#     loaded_model.trainable = False # Important for inference performance
#     global_model_broadcast = spark.sparkContext.broadcast(loaded_model)
#     print(f"\u2705 Model loaded and broadcasted from {MODEL_FILENAME}")

#     loaded_scaler = joblib.load(SCALER_FILENAME)
#     global_scaler_broadcast = spark.sparkContext.broadcast(loaded_scaler)
#     print(f"\u2705 Scaler loaded and broadcasted from {SCALER_FILENAME}")

#     with open(TICKER_MAP_FILENAME, 'r') as f:
#         loaded_ticker_map = json.load(f)
#     global_ticker_map_broadcast = spark.sparkContext.broadcast(loaded_ticker_map)
#     print(f"\u2705 Ticker mapping loaded and broadcasted from {TICKER_MAP_FILENAME}")

#     num_features = loaded_scaler.n_features_in_
#     print(f"Number of features expected by the scaler: {num_features}")

# except Exception as e:
#     print(f"\u274C Error loading artifacts: {e}")
#     spark.stop()
#     sys.exit(1)

# # --- Define Schema for Kafka Message Value ---
# # This schema must match the JSON structure produced by the Flink aggregator.
# # We'll include all expected features, ticker, timestamp, and the simulation flag.
# kafka_message_schema = StructType([
#     StructField("ticker", StringType(), True),
#     StructField("timestamp", StringType(), True), # Read as String, convert to Timestamp late
#     # Add all expected features from Flink
#     StructField("price_mean_1min", DoubleType(), True),
#     StructField("price_mean_5min", DoubleType(), True),
#     StructField("price_std_5min", DoubleType(), True),
#     StructField("price_mean_30min", DoubleType(), True),
#     StructField("price_std_30min", DoubleType(), True),
#     StructField("size_tot_1min", DoubleType(), True),
#     StructField("size_tot_5min", DoubleType(), True),
#     StructField("size_tot_30min", DoubleType(), True),
#     StructField("sentiment_bluesky_mean_2h", DoubleType(), True),
#     StructField("sentiment_bluesky_mean_1d", DoubleType(), True),
#     StructField("sentiment_news_mean_1d", DoubleType(), True),
#     StructField("sentiment_news_mean_3d", DoubleType(), True),
#     StructField("sentiment_bluesky_mean_general_2hours", DoubleType(), True),
#     StructField("sentiment_bluesky_mean_general_1d", DoubleType(), True),
#     StructField("minutes_since_open", DoubleType(), True),
#     StructField("day_of_week", IntegerType(), True),
#     StructField("day_of_month", IntegerType(), True),
#     StructField("week_of_year", IntegerType(), True),
#     StructField("month_of_year", IntegerType(), True),
#     StructField("market_open_spike_flag", BooleanType(), True),
#     StructField("market_close_spike_flag", BooleanType(), True),
#     StructField("eps", DoubleType(), True),
#     StructField("freeCashFlow", DoubleType(), True),
#     StructField("profit_margin", DoubleType(), True),
#     StructField("debt_to_equity", DoubleType(), True),
#     StructField("gdp_real", DoubleType(), True),
#     StructField("cpi", DoubleType(), True),
#     StructField("ffr", DoubleType(), True),
#     StructField("t10y", DoubleType(), True),
#     StructField("t2y", DoubleType(), True),
#     StructField("spread_10y_2y", DoubleType(), True),
#     StructField("unemployment", DoubleType(), True)
# ])

# # --- Real-time Data Buffers (Per-Executor State) ---
# # In Spark UDFs, state needs to be managed carefully.
# # We'll use a dictionary that will be local to each executor
# # and will persist across UDF calls for that executor.
# # This approach works because Spark UDFs are executed in a distributed manner,
# # and if a record for 'AAPL' arrives at executor 1, subsequent 'AAPL' records
# # might also arrive at executor 1 (due to consistent hashing for 'keyBy' if we used that,
# # but for simple UDF, it's less guaranteed. However, for a broadcast model, each executor
# # maintains its own state for the UDF's internal processing).
# # A more robust stateful streaming approach in Spark would use mapGroupsWithState
# # if we wanted Spark to manage the state explicitly.
# # For simplicity and to directly translate the existing logic:
# # `realtime_data_buffers_local` will store buffers per (ticker, offset_index) key
# # `last_timestamps_local` will store last timestamp per (ticker, offset_index) key
# # Note: These are NOT shared between executors. Each executor will maintain its own.
# # This implies that if a ticker's messages are not consistently routed to the same executor,
# # its buffer might not be correctly filled.
# # For truly consistent per-ticker state across executors, mapGroupsWithState is superior.
# # For this example, we'll assume a single executor or consistent routing for the demo.
# # Or, more practically, we use the timestamp and offset *within the UDF* to determine if a prediction
# # can be made, ensuring the buffer for that specific offset is ready.

# # Initialize these outside the UDF, but their state is specific to the executor process.
# realtime_data_buffers_local = {} # Key: (ticker_name, buffer_index) -> List of scaled features
# last_timestamps_local = {}     # Key: (ticker_name, buffer_index) -> datetime object

# # --- Prediction UDF ---
# def predict_udf_function(ticker_name_udf, timestamp_str_udf, is_simulated_udf, *feature_values):
#     """
#     User-Defined Function to handle new data, manage buffers, and make predictions.
#     This function will be executed on Spark executors.
#     """
#     # Retrieve broadcast variables within the UDF
#     model = global_model_broadcast.value
#     scaler = global_scaler_broadcast.value
#     ticker_name_to_code_map = global_ticker_map_broadcast.value
#     num_features_expected = scaler.n_features_in_ # Get it from the scaler

#     ticker_code = ticker_name_to_code_map.get(ticker_name_udf)
#     if ticker_code is None:
#         print(f"[{datetime.datetime.now()}] \u274C UDF: Ticker '{ticker_name_udf}' not found in mapping. Ignoring message.")
#         return None

#     try:
#         new_data_timestamp = datetime.datetime.fromisoformat(timestamp_str_udf)
#     except ValueError:
#         print(f"[{datetime.datetime.now()}] \u274C UDF: Invalid timestamp format: {timestamp_str_udf}. Ignoring.")
#         return None

#     second_of_minute = new_data_timestamp.second
#     if second_of_minute % 10 != 0 or second_of_minute >= NUM_OFFSET_BUFFERS * 10:
#         # print(f"[{datetime.datetime.now()}] \u274C UDF: Timestamp {new_data_timestamp} (second: {second_of_minute:02d}) not aligned to 10-second offset. Ignoring.")
#         return None
    
#     buffer_index = second_of_minute // 10
    
#     # Use a composite key for buffer and last timestamp management
#     composite_key = (ticker_name_udf, buffer_index)

#     # Initialize buffers and last timestamps for this composite key if not present
#     if composite_key not in realtime_data_buffers_local:
#         realtime_data_buffers_local[composite_key] = []
#         last_timestamps_local[composite_key] = None

#     # Check if the timestamp is ahead (to avoid out-of-order or duplicate data)
#     if last_timestamps_local[composite_key] is not None and new_data_timestamp <= last_timestamps_local[composite_key]:
#         # print(f"[{datetime.datetime.now()}] Warning: Old or duplicate data for {ticker_name_udf} (offset {buffer_index}) at {new_data_timestamp}. Ignoring.")
#         return None
    
#     # Update last timestamp
#     last_timestamps_local[composite_key] = new_data_timestamp

#     # --- Feature Extraction and Ordering ---
#     # The order of features in this list MUST match the order used when the scaler was trained.
#     # This list corresponds to the 'features' dictionary being sent by the Flink aggregator.
#     # Ensure all possible features from Flink are accounted for.
#     # If any feature is missing or None, handle it gracefully (e.g., default to 0 or mean imputation).
#     # For simplicity, we'll assume the Flink aggregator always sends all fields.
#     # If a feature might be missing, add a .get() with a default value.

#     # The *feature_values argument collects all columns after is_simulated_udf based on schema order.
#     # We need to ensure the order matches the expected_feature_keys_in_order from the original script.
#     # The schema definition ensures this.
#     current_features_for_scaling = []
#     for val in feature_values:
#         if val is None:
#             # print(f"[{datetime.datetime.now()}] \u274C Warning: Missing feature value for {ticker_name_udf}. Using 0.0.")
#             current_features_for_scaling.append(0.0)
#         else:
#             current_features_for_scaling.append(float(val)) # Ensure float type

#     new_data_features_np = np.array(current_features_for_scaling, dtype=np.float32)

#     # Verify the number of features matches what the scaler expects
#     if new_data_features_np.shape[0] != num_features_expected:
#         print(f"[{datetime.datetime.now()}] \u274C UDF: Feature mismatch for {ticker_name_udf} at {new_data_timestamp}: "
#               f"Expected {num_features_expected} features, got {new_data_features_np.shape[0]}. "
#               f"Please ensure the Flink aggregator output and the scaler's training data match feature sets and order.")
#         return None

#     # Apply the scaler to the new feature point
#     try:
#         scaled_features = scaler.transform(new_data_features_np.reshape(1, -1)).flatten()
#     except ValueError as ve:
#         print(f"[{datetime.datetime.now()}] \u274C UDF: Scaling error for {ticker_name_udf} at {new_data_timestamp}: {ve}.")
#         return None
#     except Exception as e:
#         print(f"[{datetime.datetime.now()}] \u274C UDF: Unexpected error during scaling for {ticker_name_udf} at {new_data_timestamp}: {e}")
#         return None

#     # Add the scaled features to the specific offset buffer
#     target_buffer = realtime_data_buffers_local[composite_key]
#     target_buffer.append(scaled_features)

#     # Remove the oldest data if the buffer exceeds N_STEPS
#     if len(target_buffer) > N_STEPS:
#         target_buffer.pop(0)

#     # Check if the specific buffer has enough data to predict
#     if len(target_buffer) == N_STEPS:
#         # Prepare input for the model
#         input_sequence = np.array(target_buffer).reshape(1, N_STEPS, num_features_expected)
        
#         # Prepare ticker input (1,)
#         input_ticker_code = np.array([ticker_code], dtype=np.int32)

#         # Perform the prediction
#         try:
#             prediction = model.predict([input_sequence, input_ticker_code], verbose=0)[0][0]
#             print(f"[{datetime.datetime.now()}] Prediction for {ticker_name_udf} (offset :{second_of_minute:02d}) at {new_data_timestamp} (Simulated: {is_simulated_udf}): {prediction:.4f}")
#             return float(prediction) # Return as float for Spark DoubleType
#         except Exception as e:
#             print(f"[{datetime.datetime.now()}] \u274C UDF: Error during prediction for {ticker_name_udf} at {new_data_timestamp}: {e}")
#             return None
#     else:
#         # print(f"[{datetime.datetime.now()}] Buffer for {ticker_name_udf} (offset :{second_of_minute:02d}) has {len(target_buffer)} points, needs {N_STEPS} to predict.")
#         return None

# # Register the UDF
# # The return type is DoubleType for the prediction value.
# # The input types correspond to (ticker, timestamp_str, is_simulated, feature1, feature2, ...)
# # We will dynamically pass the feature columns.
# prediction_udf = udf(predict_udf_function, DoubleType())

# # --- Read from Kafka ---
# kafka_stream_df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("startingOffsets", "latest") \
#     .load()

# # --- Process the Kafka Stream ---
# # Cast value to String and parse JSON
# parsed_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_value") \
#     .select(from_json(col("json_value"), kafka_message_schema).alias("data")) \
#     .select("data.*") # Flatten the structure

# # Dynamically create the list of feature columns to pass to the UDF
# # This ensures we pass them in the order defined by the schema after the fixed ones.
# feature_cols = [f.name for f in kafka_message_schema.fields if f.name not in ["ticker", "timestamp", "is_simulated_prediction"]]
# # Construct the arguments for the UDF
# udf_args = [col("ticker"), col("timestamp"), col("is_simulated_prediction")] + [col(f) for f in feature_cols]

# # Apply the UDF
# prediction_df = parsed_df.withColumn(
#     "predicted_price_change",
#     prediction_udf(*udf_args)
# )

# # Filter out null predictions (when buffer is not full or errors occurred)
# # And show only relevant columns
# output_df = prediction_df.filter(col("predicted_price_change").isNotNull()) \
#                          .select("ticker", col("timestamp").cast(TimestampType()).alias("event_time"), # Cast timestamp string to actual timestamp
#                                  "predicted_price_change", "is_simulated_prediction")

# # --- Write the Output to Console (for demonstration) ---
# # In a real application, you might write to another Kafka topic, Parquet, or a database.
# query = output_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# print(f"\n\U0001F535 Spark Structured Streaming query started. Listening to Kafka topic: {KAFKA_TOPIC}")
# print("Predictions will be printed below as they become available.")
# print("Press Ctrl+C to stop the stream.")

# try:
#     query.awaitTermination()
# except KeyboardInterrupt:
#     print("\n\u2705 Keyboard interrupt received. Stopping Spark Structured Streaming query.")
# except StreamingQueryException as e:
#     print(f"\u274C Streaming query exception: {e}")
# finally:
#     spark.stop()
#     print("Spark Session stopped.")