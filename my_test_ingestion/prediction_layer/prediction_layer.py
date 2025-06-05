

# #FUNZIONA MA SENZA FLINK
# import os
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from sklearn.preprocessing import MinMaxScaler
# import joblib
# import json
# import datetime
# import time
# from confluent_kafka import Consumer, KafkaException, KafkaError
# import sys

# # Paths for saving artifacts (must be the same as in the training script)
# MODEL_SAVE_PATH = "create_model_lstm/model"
# MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, "lstm_model_AAPL.h5")
# SCALER_FILENAME = os.path.join(MODEL_SAVE_PATH, "scaler_AAPL.pkl")
# TICKER_MAP_FILENAME = os.path.join(MODEL_SAVE_PATH, "ticker_map.json")

# # N_STEPS must match the N_STEPS used in training (e.g., 5 for 5 aggregated 10-second points)
# N_STEPS = 5 # Crucial: this must be the same value as in training

# # Number of offset-based buffers (every 10 seconds, from :00 to :50)
# NUM_OFFSET_BUFFERS = 6 # Corresponds to offsets :00, :10, :20, :30, :40, :50

# # --- Kafka Configuration ---
# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
# KAFKA_TOPIC = "aggregated_data" # This is the topic the Flink aggregator produces to
# KAFKA_GROUP_ID = "prediction_consumer_group"

# # --- Load Artifacts ---
# try:
#     model = tf.keras.models.load_model(MODEL_FILENAME)
#     print(f"\u2705 Model loaded from {MODEL_FILENAME}")

#     scaler = joblib.load(SCALER_FILENAME)
#     print(f"\u2705 Scaler loaded from {SCALER_FILENAME}")

#     with open(TICKER_MAP_FILENAME, 'r') as f:
#         ticker_name_to_code_map = json.load(f)
#     print(f"\u2705 Ticker mapping loaded from {TICKER_MAP_FILENAME}")

#     # The number of features expected by the scaler MUST match the number of features
#     # being sent by the Flink aggregator.
#     num_features = scaler.n_features_in_
#     print(f"Number of features expected by the scaler: {num_features}")

# except Exception as e:
#     print(f"\u274C Error loading artifacts: {e}")
#     sys.exit(1)

# ### Data Handling and Real-time Prediction

# # realtime_data_buffers will now contain a list of NUM_OFFSET_BUFFERS lists for each ticker.
# # Example: {'AAPL': [buffer_offset_00, buffer_offset_10, ..., buffer_offset_50]}
# realtime_data_buffers = {}
# # last_timestamps remains a single timestamp per ticker for order control
# last_timestamps = {}

# def get_ticker_code(ticker_name):
#     """Returns the numeric code for a ticker name."""
#     return ticker_name_to_code_map.get(ticker_name)

# def make_prediction(ticker_name, new_data_timestamp, new_data_features_dict):
#     """
#     Handles the arrival of new data and makes a prediction using the appropriate buffer.

#     Args:
#         ticker_name (str): The name of the ticker (e.g., 'AAPL').
#         new_data_timestamp (datetime.datetime): The timestamp of the new aggregated data.
#         new_data_features_dict (dict): The dictionary of aggregated features (NOT scaled) for this timestamp,
#                                        as produced by the Flink aggregator.
#     """
#     global realtime_data_buffers, last_timestamps

#     ticker_code = get_ticker_code(ticker_name)
#     if ticker_code is None:
#         print(f"\u274C Ticker '{ticker_name}' not found in mapping. Ignoring message.")
#         return None

#     # Initialize the NUM_OFFSET_BUFFERS buffers for the ticker if it's new
#     if ticker_name not in realtime_data_buffers:
#         realtime_data_buffers[ticker_name] = [[] for _ in range(NUM_OFFSET_BUFFERS)]
#         last_timestamps[ticker_name] = None
#         print(f"Initialized {NUM_OFFSET_BUFFERS} buffers for ticker: {ticker_name}")

#     # Check if the timestamp is ahead (to avoid out-of-order or duplicate data)
#     # This also handles the case where the Flink job might re-send messages on recovery,
#     # though Flink's exactly-once semantics help with that at the Flink level.
#     if last_timestamps[ticker_name] is not None and new_data_timestamp <= last_timestamps[ticker_name]:
#         # print(f"Warning: Old or duplicate data for {ticker_name} at {new_data_timestamp}. Ignoring.")
#         return None

#     # --- Feature Extraction and Ordering ---
#     # The order of features in this list MUST match the order used when the scaler was trained.
#     # This list corresponds to the 'features' dictionary being sent by the Flink aggregator.
#     # Ensure all possible features from Flink are accounted for.
#     # If any feature is missing or None, handle it gracefully (e.g., default to 0 or mean imputation).
#     # For simplicity, we'll assume the Flink aggregator always sends all fields.
#     # If a feature might be missing, add a .get() with a default value.

#     expected_feature_keys_in_order = [
#         "price_mean_1min", "price_mean_5min", "price_std_5min", "price_mean_30min", "price_std_30min",
#         "size_tot_1min", "size_tot_5min", "size_tot_30min",
#         "sentiment_bluesky_mean_2h", "sentiment_bluesky_mean_1d",
#         "sentiment_news_mean_1d", "sentiment_news_mean_3d",
#         "sentiment_bluesky_mean_general_2hours", "sentiment_bluesky_mean_general_1d",
#         "minutes_since_open", "day_of_week", "day_of_month", "week_of_year", "month_of_year",
#         "market_open_spike_flag", "market_close_spike_flag",
#         "eps", "freeCashFlow", "profit_margin", "debt_to_equity",
#         # Macro data features - these are dynamic, so we need to collect them
#         # from the macro_alias used in the Flink script.
#         "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"
#     ]
    
#     current_features_for_scaling = []
#     for key in expected_feature_keys_in_order:
#         value = new_data_features_dict.get(key)
#         if value is None:
#             # Handle missing fundamental or macro data by using a default (e.g., 0)
#             # or a more sophisticated imputation strategy if known from training.
#             # For now, print a warning and use 0.0.
#             # The model's training data should reflect how these missing values are handled.
#             print(f"\u274C Warning: Missing feature '{key}' for {ticker_name} at {new_data_timestamp}. Using 0.0.", file=sys.stderr)
#             current_features_for_scaling.append(0.0)
#         else:
#             current_features_for_scaling.append(float(value)) # Ensure float type

#     new_data_features_np = np.array(current_features_for_scaling, dtype=np.float32)

#     # Verify the number of features matches what the scaler expects
#     if new_data_features_np.shape[0] != num_features:
#         print(f"\u274C Feature mismatch for {ticker_name} at {new_data_timestamp}: "
#               f"Expected {num_features} features, got {new_data_features_np.shape[0]}. "
#               f"Please ensure the Flink aggregator output and the scaler's training data match feature sets and order.")
#         return None

#     # Apply the scaler to the new feature point
#     try:
#         scaled_features = scaler.transform(new_data_features_np.reshape(1, -1)).flatten()
#     except ValueError as ve:
#         print(f"\u274C Scaling error for {ticker_name} at {new_data_timestamp}: {ve}. Ensure features have the correct dimension ({num_features}).")
#         return None
#     except Exception as e:
#         print(f"\u274C Unexpected error during scaling for {ticker_name} at {new_data_timestamp}: {e}")
#         return None

#     # Determine the buffer index based on the second of the timestamp
#     # Flink aggregator guarantees seconds will be 00, 10, ..., 50
#     second_of_minute = new_data_timestamp.second
    
#     # Ensure the second is a multiple of 10 and within the valid range (0-50)
#     if second_of_minute % 10 != 0 or second_of_minute >= NUM_OFFSET_BUFFERS * 10:
#         print(f"\u274C Warning: Timestamp {new_data_timestamp} (second: {second_of_minute:02d}) is not aligned to an expected 10-second offset (00, 10, ..., 50). Ignoring the point.")
#         return None
    
#     buffer_index = second_of_minute // 10 # This correctly maps 00->0, 10->1, 20->2, etc.

#     # Add the scaled features to the specific offset buffer
#     current_ticker_buffers = realtime_data_buffers[ticker_name]
#     target_buffer = current_ticker_buffers[buffer_index]
    
#     target_buffer.append(scaled_features)

#     # Remove the oldest data if the buffer exceeds N_STEPS
#     if len(target_buffer) > N_STEPS:
#         target_buffer.pop(0)

#     last_timestamps[ticker_name] = new_data_timestamp

#     # Check if the specific buffer (the one just updated) has enough data to predict
#     if len(target_buffer) == N_STEPS:
#         # Prepare input for the model
#         # input_sequence must be (1, N_STEPS, num_features)
#         input_sequence = np.array(target_buffer).reshape(1, N_STEPS, num_features)
        
#         # Prepare ticker input (1,)
#         input_ticker_code = np.array([ticker_code], dtype=np.int32)

#         # Perform the prediction
#         try:
#             prediction = model.predict([input_sequence, input_ticker_code], verbose=0)[0][0]
#             #prediction = model.predict(input_sequence, verbose=0)[0][0]
#             # Also log the is_simulated_prediction flag from the Flink output
#             is_simulated = new_data_features_dict.get("is_simulated_prediction", False)
#             print(f"Prediction for {ticker_name} (offset :{second_of_minute:02d}) at {new_data_timestamp} (Simulated: {is_simulated}): {prediction:.4f}")
#             return prediction
#         except Exception as e:
#             print(f"\u274C Error during prediction for {ticker_name} at {new_data_timestamp}: {e}")
#             return None
#     else:
#         # print(f"Buffer for {ticker_name} (offset :{second_of_minute:02d}) has {len(target_buffer)} points, needs {N_STEPS} to predict.")
#         return None

# if __name__ == "__main__":
#     print("\n\U0001F535 Starting Kafka consumer for real-time predictions...")

#     conf = {
#         'bootstrap.servers': KAFKA_BROKER,
#         'group.id': KAFKA_GROUP_ID,
#         'auto.offset.reset': 'latest'
#     }

#     consumer = Consumer(conf)

#     try:
#         consumer.subscribe([KAFKA_TOPIC])
#         print(f"\u2705 Subscribed to Kafka topic: {KAFKA_TOPIC}")

#         print("\nWaiting for messages from Kafka...")
#         while True:
#             msg = consumer.poll(timeout=1.0)

#             if msg is None:
#                 continue
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
#                                      (msg.topic(), msg.partition(), msg.offset()))
#                 elif msg.error():
#                     raise KafkaException(msg.error())
#             else:
#                 try:
#                     data = json.loads(msg.value().decode('utf-8'))
                    
#                     ticker_name = data.get('ticker')
#                     timestamp_str = data.get('timestamp')
#                     # The 'features' are now a dictionary from the Flink aggregator
#                     features_dict = {k: v for k, v in data.items() if k not in ['ticker', 'timestamp', 'is_simulated_prediction']}
#                     # Also explicitly extract is_simulated_prediction if you want to pass it
#                     is_simulated_prediction_flag = data.get('is_simulated_prediction', False)


#                     if not all([ticker_name, timestamp_str, features_dict is not None]):
#                         print(f"\u274C Malformed Kafka message: {data}. Expected 'ticker', 'timestamp', and aggregated features.")
#                         continue

#                     # Convert timestamp to datetime object
#                     # Flink uses ISO format, so datetime.fromisoformat is appropriate
#                     new_data_timestamp = datetime.datetime.fromisoformat(timestamp_str)
                    
#                     # Pass the features_dict directly to make_prediction
#                     make_prediction(ticker_name, new_data_timestamp, data) # Pass the whole dict to get all features including flag

#                 except json.JSONDecodeError:
#                     print(f"\u274C JSON decoding error from Kafka message: {msg.value()}")
#                 except Exception as e:
#                     print(f"\u274C Error processing Kafka message: {e} - Message: {msg.value()}")

#     except KeyboardInterrupt:
#         print("\n\u2705 Keyboard interrupt. Closing Kafka consumer.")
#     finally:
#         consumer.close()
#         print("Kafka consumer closed.")





# import os
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from sklearn.preprocessing import MinMaxScaler
# import joblib
# import json
# import datetime
# import time
# from confluent_kafka import Consumer, KafkaException, KafkaError, Producer # Importa Producer
# import sys

# # Paths for saving artifacts
# MODELS_BASE_PATH = "create_model_lstm/models"  # Cartella che contiene tutti i modelli
# SCALERS_BASE_PATH = "create_model_lstm/scalers" # Cartella che contiene tutti gli scalers

# # TICKER_MAP_FILENAME rimane, ma il suo uso potrebbe cambiare se ogni modello ha il suo input specifico
# # TICKER_MAP_FILENAME = os.path.join(MODEL_SAVE_PATH, "ticker_map.json") # Potrebbe non servire se i modelli sono per singolo ticker

# # N_STEPS must match the N_STEPS used in training (e.g., 5 for 5 aggregated 10-second points)
# N_STEPS = 5 # Crucial: this must be the same value as in training

# # Number of offset-based buffers (every 10 seconds, from :00 to :50)
# NUM_OFFSET_BUFFERS = 6 # Corresponds to offsets :00, :10, :20, :30, :40, :50

# # --- Kafka Configuration ---
# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
# KAFKA_TOPIC_INPUT = "aggregated_data" # Topic da cui leggiamo i dati aggregati
# KAFKA_TOPIC_OUTPUT = "prediction"     # Nuovo topic per inviare le previsioni
# KAFKA_GROUP_ID = "prediction_consumer_group"

# # --- Cache per Modelli e Scalers ---
# loaded_models = {}
# loaded_scalers = {}

# # --- Funzioni per ottenere i percorsi dei file ---
# def get_model_path(ticker_name):
#     return os.path.join(MODELS_BASE_PATH, f"lstm_model_{ticker_name}.h5")

# def get_scaler_path(ticker_name):
#     return os.path.join(SCALERS_BASE_PATH, f"scaler_{ticker_name}.pkl")

# # --- Funzione per caricare dinamicamente Modelli e Scalers ---
# def load_model_and_scaler_for_ticker(ticker_name):
#     """Carica il modello e lo scaler per un dato ticker, con caching."""
#     if ticker_name not in loaded_models:
#         model_path = get_model_path(ticker_name)
#         scaler_path = get_scaler_path(ticker_name)
#         try:
#             model = tf.keras.models.load_model(model_path)
#             scaler = joblib.load(scaler_path)
#             loaded_models[ticker_name] = model
#             loaded_scalers[ticker_name] = scaler
#             num_features = scaler.n_features_in_
#             print(f"\u2705 Loaded model and scaler for {ticker_name} from {model_path} and {scaler_path}. Features: {num_features}")
#         except Exception as e:
#             print(f"\u274C Error loading artifacts for {ticker_name}: {e}")
#             return None, None, None
    
#     return loaded_models[ticker_name], loaded_scalers[ticker_name], loaded_scalers[ticker_name].n_features_in_

# ### Data Handling e Real-time Prediction

# # realtime_data_buffers will now contain a list of NUM_OFFSET_BUFFERS lists for each ticker.
# # Example: {'AAPL': [buffer_offset_00, buffer_offset_10, ..., buffer_offset_50]}
# realtime_data_buffers = {}
# # last_timestamps remains a single timestamp per ticker for order control
# last_timestamps = {}

# # Non useremo più get_ticker_code se ogni modello è per singolo ticker
# # def get_ticker_code(ticker_name):
# #     """Returns the numeric code for a ticker name."""
# #     return ticker_name_to_code_map.get(ticker_name)

# def make_prediction(ticker_name, new_data_timestamp, new_data_features_dict, kafka_producer):
#     """
#     Handles the arrival of new data and makes a prediction using the appropriate buffer.
#     Sends the prediction to the dashboard Kafka topic.

#     Args:
#         ticker_name (str): The name of the ticker (e.g., 'AAPL').
#         new_data_timestamp (datetime.datetime): The timestamp of the new aggregated data.
#         new_data_features_dict (dict): The dictionary of aggregated features (NOT scaled) for this timestamp,
#                                        as produced by the Flink aggregator.
#         kafka_producer (Producer): The Kafka producer instance to send messages.
#     """
#     global realtime_data_buffers, last_timestamps

#     # Carica dinamicamente il modello, lo scaler e num_features per questo ticker
#     model_for_ticker, scaler_for_ticker, num_features_for_ticker = load_model_and_scaler_for_ticker(ticker_name)
#     if model_for_ticker is None or scaler_for_ticker is None:
#         print(f"\u274C Cannot make prediction for {ticker_name} due to missing model/scaler.")
#         return None

#     # Initialize the NUM_OFFSET_BUFFERS buffers for the ticker if it's new
#     if ticker_name not in realtime_data_buffers:
#         realtime_data_buffers[ticker_name] = [[] for _ in range(NUM_OFFSET_BUFFERS)]
#         last_timestamps[ticker_name] = None
#         print(f"Initialized {NUM_OFFSET_BUFFERS} buffers for ticker: {ticker_name}")

#     # Check if the timestamp is ahead (to avoid out-of-order or duplicate data)
#     if last_timestamps[ticker_name] is not None and new_data_timestamp <= last_timestamps[ticker_name]:
#         # print(f"Warning: Old or duplicate data for {ticker_name} at {new_data_timestamp}. Ignoring.")
#         return None

#     # --- Feature Extraction and Ordering ---
#     expected_feature_keys_in_order = [
#         "price_mean_1min", "price_mean_5min", "price_std_5min", "price_mean_30min", "price_std_30min",
#         "size_tot_1min", "size_tot_5min", "size_tot_30min",
#         "sentiment_bluesky_mean_2h", "sentiment_bluesky_mean_1d",
#         "sentiment_news_mean_1d", "sentiment_news_mean_3d",
#         "sentiment_bluesky_mean_general_2hours", "sentiment_bluesky_mean_general_1d",
#         "minutes_since_open", "day_of_week", "day_of_month", "week_of_year", "month_of_year",
#         "market_open_spike_flag", "market_close_spike_flag",
#         "eps", "freeCashFlow", "profit_margin", "debt_to_equity",
#         "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"
#     ]
    
#     current_features_for_scaling = []
#     for key in expected_feature_keys_in_order:
#         value = new_data_features_dict.get(key)
#         if value is None:
#             print(f"\u274C Warning: Missing feature '{key}' for {ticker_name} at {new_data_timestamp}. Using 0.0.", file=sys.stderr)
#             current_features_for_scaling.append(0.0)
#         else:
#             current_features_for_scaling.append(float(value))

#     new_data_features_np = np.array(current_features_for_scaling, dtype=np.float32)

#     # Verify the number of features matches what the *current scaler* expects
#     if new_data_features_np.shape[0] != num_features_for_ticker:
#         print(f"\u274C Feature mismatch for {ticker_name} at {new_data_timestamp}: "
#               f"Expected {num_features_for_ticker} features, got {new_data_features_np.shape[0]}. "
#               f"Please ensure the Flink aggregator output and the scaler's training data match feature sets and order.")
#         return None

#     # Apply the *ticker-specific* scaler to the new feature point
#     try:
#         scaled_features = scaler_for_ticker.transform(new_data_features_np.reshape(1, -1)).flatten()
#     except ValueError as ve:
#         print(f"\u274C Scaling error for {ticker_name} at {new_data_timestamp}: {ve}. Ensure features have the correct dimension ({num_features_for_ticker}).")
#         return None
#     except Exception as e:
#         print(f"\u274C Unexpected error during scaling for {ticker_name} at {new_data_timestamp}: {e}")
#         return None

#     # Determine the buffer index based on the second of the timestamp
#     second_of_minute = new_data_timestamp.second
    
#     if second_of_minute % 10 != 0 or second_of_minute >= NUM_OFFSET_BUFFERS * 10:
#         print(f"\u274C Warning: Timestamp {new_data_timestamp} (second: {second_of_minute:02d}) is not aligned to an expected 10-second offset (00, 10, ..., 50). Ignoring the point.")
#         return None
    
#     buffer_index = second_of_minute // 10

#     current_ticker_buffers = realtime_data_buffers[ticker_name]
#     target_buffer = current_ticker_buffers[buffer_index]
    
#     target_buffer.append(scaled_features)

#     # Remove the oldest data if the buffer exceeds N_STEPS
#     if len(target_buffer) > N_STEPS:
#         target_buffer.pop(0)

#     last_timestamps[ticker_name] = new_data_timestamp

#     # Check if the specific buffer has enough data to predict
#     if len(target_buffer) == N_STEPS:
#         input_sequence = np.array(target_buffer).reshape(1, N_STEPS, num_features_for_ticker)
        
#         # Perform the prediction using the *ticker-specific* model
#         try:
#             # Rimuovi l'input del ticker code se il modello è per singolo ticker
#             prediction = model_for_ticker.predict(input_sequence, verbose=0)[0][0]
#             is_simulated = new_data_features_dict.get("is_simulated_prediction", False)
            
#             predicted_for_timestamp = new_data_timestamp + datetime.timedelta(seconds=60)

#             prediction_data = {
#                 "ticker": ticker_name,
#                 "timestamp": predicted_for_timestamp.isoformat(),
#                 "prediction": float(prediction), # Assicurati che sia un float serializzabile
#                 "is_simulated_prediction": is_simulated
#             }
            
#             # Send prediction to Kafka dashboard topic
#             try:
#                 kafka_producer.produce(KAFKA_TOPIC_OUTPUT, key=ticker_name, value=json.dumps(prediction_data).encode('utf-8'))
#                 kafka_producer.poll(0) # Poll per gestire i callback asincroni (potrebbe non essere strettamente necessario qui)
#                 print(f"Prediction for {ticker_name} (offset :{second_of_minute:02d}) at {new_data_timestamp} (Simulated: {is_simulated}): {prediction:.4f} \u27A1 Sent to '{KAFKA_TOPIC_OUTPUT}'")
#             except Exception as kafka_e:
#                 print(f"\u274C Error sending message to Kafka dashboard topic: {kafka_e}")

#             return prediction
#         except Exception as e:
#             print(f"\u274C Error during prediction for {ticker_name} at {new_data_timestamp}: {e}")
#             return None
#     else:
#         # print(f"Buffer for {ticker_name} (offset :{second_of_minute:02d}) has {len(target_buffer)} points, needs {N_STEPS} to predict.")
#         return None

# if __name__ == "__main__":
#     print("\n\U0001F535 Starting Kafka consumer for real-time predictions...")

#     # Kafka Consumer Configuration
#     consumer_conf = {
#         'bootstrap.servers': KAFKA_BROKER,
#         'group.id': KAFKA_GROUP_ID,
#         'auto.offset.reset': 'latest'
#     }

#     # Kafka Producer Configuration
#     producer_conf = {
#         'bootstrap.servers': KAFKA_BROKER
#     }

#     consumer = Consumer(consumer_conf)
#     producer = Producer(producer_conf) # Inizializza il producer

#     try:
#         consumer.subscribe([KAFKA_TOPIC_INPUT])
#         print(f"\u2705 Subscribed to Kafka topic: {KAFKA_TOPIC_INPUT}")
#         print(f"\u2705 Will publish predictions to Kafka topic: {KAFKA_TOPIC_OUTPUT}")

#         print("\nWaiting for messages from Kafka...")
#         while True:
#             msg = consumer.poll(timeout=1.0)

#             if msg is None:
#                 continue
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
#                                      (msg.topic(), msg.partition(), msg.offset()))
#                 elif msg.error():
#                     raise KafkaException(msg.error())
#             else:
#                 try:
#                     data = json.loads(msg.value().decode('utf-8'))
                    
#                     ticker_name = data.get('ticker')
#                     timestamp_str = data.get('timestamp')
#                     # Pass the whole dict to make_prediction to extract all features and the flag
                    
#                     if not all([ticker_name, timestamp_str]): # features_dict is now derived inside make_prediction
#                         print(f"\u274C Malformed Kafka message: {data}. Expected 'ticker' and 'timestamp'.")
#                         continue

#                     # Convert timestamp to datetime object
#                     new_data_timestamp = datetime.datetime.fromisoformat(timestamp_str)
                    
#                     # Pass the producer instance to make_prediction
#                     make_prediction(ticker_name, new_data_timestamp, data, producer)

#                 except json.JSONDecodeError:
#                     print(f"\u274C JSON decoding error from Kafka message: {msg.value()}")
#                 except Exception as e:
#                     print(f"\u274C Error processing Kafka message: {e} - Message: {msg.value()}")

#     except KeyboardInterrupt:
#         print("\n\u2705 Keyboard interrupt. Closing Kafka consumer and producer.")
#     finally:
#         consumer.close()
#         producer.flush() # Assicurati che tutti i messaggi in coda siano inviati prima di chiudere
#         print("Kafka consumer and producer closed.")






























# #FLINK FLINK FLINK 
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
# import joblib




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
#                 self.scaler = joblib.load(f)
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
# import json
# import numpy as np
# import tensorflow as tf
# import pickle
# from datetime import datetime, timedelta
# import logging
# import time
# import sys # Importa sys per print a stderr

# # Importa KafkaAdminClient e le eccezioni necessarie
# from kafka import KafkaAdminClient, KafkaConsumer
# from kafka.errors import NoBrokersAvailable, KafkaError, UnknownTopicOrPartitionError

# from pyflink.common import WatermarkStrategy, Duration, Row, RestartStrategies
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
# from pyflink.datastream.connectors import FlinkKafkaProducer
# from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.checkpointing_mode import CheckpointingMode
# import joblib
# from pyflink.datastream.execution_mode import RuntimeExecutionMode
# from pyflink.datastream.state import MapStateDescriptor


# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Base paths for models and scalers inside the Flink container
# MODELS_BASE_PATH = "/opt/flink/models"
# SCALERS_BASE_PATH = "/opt/flink/scalers"

# # N_STEPS must match the N_STEPS used in training (e.g., 5 for 5 aggregated 10-second points)
# N_STEPS = 5

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
#     """Safely converts a value to float, defaulting to 0.0 on error."""
#     try:
#         return float(x)
#     except (ValueError, TypeError):
#         return 0.0

# class LSTMPredictionFunction(KeyedProcessFunction):
#     """
#     KeyedProcessFunction to handle real-time stock price predictions using
#     per-ticker LSTM models loaded dynamically.
#     """
#     def open(self, runtime_context: RuntimeContext):
#         descriptor = MapStateDescriptor(
#             "buffer",
#             Types.LONG(), # La chiave della mappa di stato interna sarà il timestamp
#             Types.PICKLED_BYTE_ARRAY() # Il valore sarà la lista di feature serializzata
#         )
#         self.buffer_state = runtime_context.get_map_state(descriptor)
#         logger.info("LSTMPredictionFunction opened successfully.")
#         self.loaded_models = {}
#         self.loaded_scalers = {}
#         self.num_features_per_ticker = {}

#     def _get_model_path(self, ticker_name):
#         return os.path.join(MODELS_BASE_PATH, f"lstm_model_{ticker_name}.h5")

#     def _get_scaler_path(self, ticker_name):
#         return os.path.join(SCALERS_BASE_PATH, f"scaler_{ticker_name}.pkl")

#     def _load_model_and_scaler_for_ticker(self, ticker_name):
#         """
#         Loads model and scaler for a given ticker, with caching.
#         Returns (model, scaler, num_features) or (None, None, None) on failure.
#         """
#         if ticker_name not in self.loaded_models:
#             model_path = self._get_model_path(ticker_name)
#             scaler_path = self._get_scaler_path(ticker_name)
#             try:
#                 model = tf.keras.models.load_model(model_path)
#                 with open(scaler_path, 'rb') as f:
#                     scaler = joblib.load(f)
                
#                 self.loaded_models[ticker_name] = model
#                 self.loaded_scalers[ticker_name] = scaler
#                 self.num_features_per_ticker[ticker_name] = scaler.n_features_in_ if hasattr(scaler, 'n_features_in_') else len(FEATURE_COLUMNS_ORDERED)
#                 logger.info(f"\u2705 Loaded model and scaler for {ticker_name} from {model_path} and {scaler_path}. Features: {self.num_features_per_ticker[ticker_name]}")
#             except Exception as e:
#                 logger.error(f"\u274C Error loading artifacts for {ticker_name}: {e}", exc_info=True)
#                 return None, None, None
        
#         return (self.loaded_models.get(ticker_name), 
#                 self.loaded_scalers.get(ticker_name), 
#                 self.num_features_per_ticker.get(ticker_name))

#     def process_element(self, value: Row, ctx: 'KeyedProcessFunction.Context'):
#         try:
#             ticker_name = value["ticker"]
#             timestamp_str = value["timestamp"]
#             timestamp_dt = datetime.fromisoformat(timestamp_str) 

#             logger.debug(f"Processing element for ticker: {ticker_name}, timestamp: {timestamp_str}")

#             model, scaler, num_features = self._load_model_and_scaler_for_ticker(ticker_name)
#             if model is None or scaler is None:
#                 logger.warning(f"Skipping prediction for {ticker_name} due to missing model/scaler or loading error.")
#                 return

#             current_features_for_scaling = []
#             for col in FEATURE_COLUMNS_ORDERED:
#                 current_features_for_scaling.append(safe_float(value.get(col, 0.0)))
            
#             current_features_np = np.array(current_features_for_scaling, dtype=np.float32)

#             if current_features_np.shape[0] != num_features:
#                 logger.error(f"Feature count mismatch for {ticker_name} at {timestamp_str}. "
#                              f"Expected {num_features}, got {current_features_np.shape[0]}. Skipping.")
#                 return

#             # Retrieve the current state for the ticker
#             # The key for MapState is implicitly the key_by value (ticker_name for this function).
#             # The internal map within the state stores timestamp_str -> features_list.
#             # State value is pickled, so load it
#             current_ticker_buffer_map_bytes = self.buffer_state.get() # No key needed for MapState for keyed functions
#             current_ticker_buffer_map = pickle.loads(current_ticker_buffer_map_bytes) if current_ticker_buffer_map_bytes else {}
            
#             # Store the current unscaled feature list with its timestamp
#             current_ticker_buffer_map[timestamp_str] = current_features_for_scaling
            
#             # Sort the buffer by timestamp and keep only the latest N_STEPS elements
#             sorted_buffer_items = sorted(current_ticker_buffer_map.items(), key=lambda item: item[0], reverse=True)[:N_STEPS]
            
#             # Update the state with the pruned buffer. Convert back to dict if sorted_buffer_items is a list of tuples.
#             # State value is pickled, so dump it
#             self.buffer_state.put(pickle.dumps(dict(sorted_buffer_items))) # No key needed for MapState for keyed functions
            
#             if len(sorted_buffer_items) == N_STEPS:
#                 logger.info(f"Buffer full ({N_STEPS} points) for ticker {ticker_name}. Performing prediction.")
                
#                 sequence_for_scaling = []
#                 # Re-sort in ascending order by timestamp for correct sequence for LSTM
#                 for ts_key, features_list in sorted(sorted_buffer_items, key=lambda item: item[0]):
#                     sequence_for_scaling.append(features_list)
                
#                 features_np_sequence = np.array(sequence_for_scaling, dtype=np.float32)
                
#                 # Check if the number of features matches the scaler's expectation
#                 if features_np_sequence.shape[1] != num_features:
#                     logger.error(f"Shape mismatch for scaling {ticker_name}. Expected {num_features} features per step, got {features_np_sequence.shape[1]}. Skipping prediction.")
#                     return

#                 scaled_2d = scaler.transform(features_np_sequence.reshape(-1, num_features))
                
#                 scaled_input_lstm = scaled_2d.reshape(1, N_STEPS, num_features)
                
#                 logger.debug(f"Scaled LSTM input shape for {ticker_name}: {scaled_input_lstm.shape}")

#                 prediction = model.predict(scaled_input_lstm, verbose=0)[0][0]
#                 logger.info(f"Prediction for {ticker_name} (based on {timestamp_str}): {prediction}")

#                 predicted_for_timestamp = timestamp_dt + timedelta(seconds=60)
                
#                 prediction_output = {
#                     "ticker": ticker_name,
#                     "timestamp": predicted_for_timestamp.isoformat(timespec='seconds'),
#                     "prediction": float(prediction)
#                 }

#                 yield Row(**prediction_output)
                
#                 # Clear the buffer state for this ticker after a prediction
#                 self.buffer_state.clear() # No key needed for MapState for keyed functions
#                 logger.debug(f"Buffer reset for ticker: {ticker_name}")

#         except Exception as e:
#             logger.error(f"Error processing element for key {ctx.get_current_key()}: {e}", exc_info=True)


# # === 🔄 Funzioni di attesa Kafka migliorate ===
# def wait_for_kafka_topics(required_topics, kafka_broker, timeout=5, max_retries=60):
#     """
#     Attende che il broker Kafka sia disponibile e che tutti i topic necessari esistano.
#     """
#     retry_count = 0
#     admin = None
    
#     while retry_count < max_retries:
#         try:
#             # KafkaAdminClient per controllare i topic
#             admin = KafkaAdminClient(
#                 bootstrap_servers=kafka_broker, 
#                 request_timeout_ms=10000,
#                 api_version=(0, 10, 2)  # Specifica una versione API compatibile
#             )
            
#             # Ottieni i metadati dei topic
#             topics_metadata = admin.list_topics()
            
#             # Gestisci diversi formati di risposta
#             if isinstance(topics_metadata, dict):
#                 available_topics = list(topics_metadata.keys())
#             elif hasattr(topics_metadata, 'topics'):
#                 available_topics = list(topics_metadata.topics.keys())
#             else:
#                 available_topics = list(topics_metadata)
            
#             logger.info(f"📋 Topic disponibili: {sorted(available_topics)}")
            
#             # Verifica se tutti i topic richiesti sono presenti
#             missing_topics = [t for t in required_topics if t not in available_topics]
            
#             if not missing_topics:
#                 logger.info(f"✅ Kafka è pronto con tutti i topic richiesti: {required_topics}.")
#                 admin.close()
#                 return True
#             else:
#                 logger.warning(f"⏳ Tentativo {retry_count + 1}/{max_retries} - Topic mancanti: {missing_topics}")
                
#         except NoBrokersAvailable:
#             logger.warning(f"⏳ Tentativo {retry_count + 1}/{max_retries} - Nessun Kafka broker disponibile a {kafka_broker}")
#         except Exception as e:
#             logger.warning(f"⏳ Tentativo {retry_count + 1}/{max_retries} - Errore: {str(e)}")
#         finally:
#             if admin:
#                 try:
#                     admin.close()
#                 except Exception:
#                     pass
#                 admin = None
        
#         retry_count += 1
#         if retry_count < max_retries:
#             time.sleep(timeout)
#         else:
#             logger.error(f"❌ Raggiunto il numero massimo di tentativi ({max_retries}). Impossibile trovare tutti i topic richiesti.")
#             return False
    
#     return False


# def wait_for_kafka_topics_with_fallback(required_topics, kafka_broker, timeout=5, max_retries=12):
#     """
#     Versione con fallback che procede comunque dopo un certo numero di tentativi.
#     """
#     if wait_for_kafka_topics(required_topics, kafka_broker, timeout, max_retries):
#         return True
    
#     logger.warning(f"⚠️  Procedo comunque senza conferma della disponibilità dei topic: {required_topics}")
#     logger.warning(f"⚠️  Se il topic non esiste, il job Flink potrebbe fallire durante l'esecuzione.")
#     return False


# def verify_topic_with_consumer(topic_name, kafka_broker):
#     """
#     Verifica l'esistenza di un topic usando un consumer Kafka.
#     """
#     consumer = None
#     try:
#         consumer = KafkaConsumer(
#             bootstrap_servers=kafka_broker,
#             consumer_timeout_ms=5000,
#             auto_offset_reset='latest'
#         )
        
#         # Prova a ottenere i metadati del topic
#         partitions = consumer.partitions_for_topic(topic_name)
        
#         if partitions:
#             logger.info(f"✅ Topic '{topic_name}' verificato con consumer. Partizioni: {len(partitions)}")
#             return True
#         else:
#             logger.warning(f"⚠️  Topic '{topic_name}' non trovato o senza partizioni.")
#             return False
            
#     except UnknownTopicOrPartitionError:
#         logger.warning(f"⚠️  Topic '{topic_name}' non esiste.")
#         return False
#     except Exception as e:
#         logger.error(f"❌ Errore durante la verifica del topic '{topic_name}': {e}")
#         return False
#     finally:
#         if consumer:
#             try:
#                 consumer.close()
#             except Exception:
#                 pass


# def debug_kafka_topics(kafka_broker):
#     """
#     Debug dettagliato dei topic Kafka disponibili.
#     """
#     logger.info("🔍 Iniziando debug dettagliato dei topic Kafka...")
    
#     # Metodo 1: AdminClient
#     try:
#         admin = KafkaAdminClient(bootstrap_servers=kafka_broker, request_timeout_ms=10000)
#         topics_metadata = admin.list_topics()
        
#         if isinstance(topics_metadata, dict):
#             admin_topics = list(topics_metadata.keys())
#         elif hasattr(topics_metadata, 'topics'):
#             admin_topics = list(topics_metadata.topics.keys())
#         else:
#             admin_topics = list(topics_metadata)
            
#         logger.info(f"🔍 AdminClient - Topic trovati: {sorted(admin_topics)}")
#         admin.close()
#     except Exception as e:
#         logger.error(f"🔍 AdminClient fallito: {e}")
    
#     # Metodo 2: Consumer
#     try:
#         consumer = KafkaConsumer(bootstrap_servers=kafka_broker, consumer_timeout_ms=5000)
#         consumer_topics = consumer.topics()
#         logger.info(f"🔍 Consumer - Topic trovati: {sorted(consumer_topics)}")
#         consumer.close()
#     except Exception as e:
#         logger.error(f"🔍 Consumer fallito: {e}")


# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
#     env.enable_checkpointing(60000, CheckpointingMode.EXACTLY_ONCE)
#     env.get_checkpoint_config().set_checkpoint_timeout(300000)
#     env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
#     env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    
#     env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
#         restart_attempts=3,
#         delay_between_attempts=timedelta(seconds=10)
#     ))
    
#     env.set_parallelism(1)

#     KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
#     KAFKA_TOPIC_AGGREGATED = os.getenv("KAFKA_TOPIC_AGGREGATED", "prova")
#     KAFKA_TOPIC_DASHBOARD = os.getenv("KAFKA_TOPIC_DASHBOARD", "prediction")

#     logger.info(f"Kafka Broker: {KAFKA_BROKER}")
#     logger.info(f"Kafka Input Topic: {KAFKA_TOPIC_AGGREGATED}")
#     logger.info(f"Kafka Output Topic: {KAFKA_TOPIC_DASHBOARD}")

#     # === Verifica connessione Kafka (opzionale) ===
#     logger.info("🔄 Verifico solo la connessione al broker Kafka...")
    
#     try:
#         # Verifica solo che il broker sia raggiungibile
#         admin = KafkaAdminClient(
#             bootstrap_servers=KAFKA_BROKER, 
#             request_timeout_ms=5000
#         )
#         admin.list_topics()  # Chiamata veloce per testare la connessione
#         admin.close()
#         logger.info("✅ Connessione a Kafka stabilita con successo.")
#     except Exception as e:
#         logger.warning(f"⚠️  Problemi di connessione a Kafka: {e}")
#         logger.warning("⚠️  Procedo comunque - Flink ritenterà automaticamente.")
    
#     logger.info("🚀 Configurando il job Flink...")
#     logger.info("ℹ️  Flink attenderà automaticamente che i topic siano disponibili.")

#     input_column_names = ["ticker", "timestamp"] + FEATURE_COLUMNS_ORDERED
#     input_column_types = [Types.STRING(), Types.STRING()] + [Types.FLOAT()] * len(FEATURE_COLUMNS_ORDERED)

#     kafka_deserialization_schema = JsonRowDeserializationSchema.builder() \
#         .type_info(Types.ROW_NAMED(input_column_names, input_column_types)).build()

#     kafka_source = KafkaSource.builder() \
#         .set_bootstrap_servers(KAFKA_BROKER) \
#         .set_topics(KAFKA_TOPIC_AGGREGATED) \
#         .set_group_id("lstm_prediction_flink_group") \
#         .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
#         .set_value_only_deserializer(kafka_deserialization_schema) \
#         .build()

#     data_stream = env.from_source(
#         source=kafka_source,
#         watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
#         source_name="Kafka_Aggregated_Source"
#     )

#     producer_serialization_schema = JsonRowSerializationSchema.builder() \
#         .with_type_info(
#             Types.ROW_NAMED(
#                 ["ticker", "timestamp", "prediction"],
#                 [Types.STRING(), Types.STRING(), Types.FLOAT()]
#             )
#         ).build()

#     kafka_producer = FlinkKafkaProducer(
#         KAFKA_TOPIC_DASHBOARD,
#         producer_serialization_schema,
#         {'bootstrap.servers': KAFKA_BROKER}
#     )

#     predicted_stream = data_stream.key_by(lambda x: x["ticker"]).process(LSTMPredictionFunction())
    
#     predicted_stream.print("Predictions Output")
    
#     predicted_stream.add_sink(kafka_producer).name("Kafka_Dashboard_Sink")

#     logger.info("🚀 Starting Flink job...")
#     env.execute("LSTM Stock Price Prediction Job (Per-Ticker Models)")

# if __name__ == "__main__":
#     main()



















import os
import json
import logging
from datetime import datetime, timedelta

from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor

# Suppress TensorFlow logging warnings (optional, useful for cleaner console output)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow as tf
from tensorflow.keras.models import load_model
import joblib
import numpy as np

# Configure logging for better visibility in Flink logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configurazione ---
# Sostituisci con i tuoi broker Kafka effettivi
KAFKA_BROKERS = 'localhost:9092'
INPUT_TOPIC = 'aggregated_data'
OUTPUT_TOPIC = 'predicted_prices'

# Percorsi base per modelli e scaler. Assicurati che queste cartelle siano
# accessibili all'interno del container Docker o tramite la cache distribuita di Flink.
MODEL_BASE_PATH = '/opt/flink/models'
SCALER_BASE_PATH = '/opt/flink/scalers'

# Numero di lag richiesti dal modello LSTM (5 minuti precedenti)
N_LAG_STEPS = 5

# Definizione dello schema dei dati in ingresso da Kafka
# 'timestamp' è un timestamp Unix in millisecondi
INPUT_SCHEMA = Types.ROW([
    ("ticker", Types.STRING()),
    ("timestamp", Types.LONG()),
    ("price", Types.FLOAT())
])

# Definizione dello schema dei dati in uscita per Kafka
OUTPUT_SCHEMA = Types.ROW([
    ("target_timestamp", Types.LONG()), # Timestamp Unix in millisecondi
    ("predicted_price", Types.FLOAT()),
    ("ticker", Types.STRING())
])

class PredictionProcessFunction(KeyedProcessFunction):
    """
    Una KeyedProcessFunction per gestire lo stato per ticker, caricare modelli
    e scaler, e fare predizioni in tempo reale.
    """

    def open(self, runtime_context):
        """
        Inizializza lo stato e carica il modello/scaler per il ticker corrente.
        Questo metodo viene chiamato una volta per task Flink per ogni chiave unica (ticker).
        """
        self.ticker = runtime_context.get_current_key()
        logger.info(f"Inizializzazione PredictionProcessFunction per il ticker: {self.ticker}")

        # Stato per memorizzare la cronologia dei prezzi per ogni intervallo di 10 secondi.
        # La chiave è il secondo (0, 10, 20, 30, 40, 50).
        # Il valore è una lista di float (i prezzi storici), serializzata come stringa JSON.
        self.price_history_by_second = runtime_context.get_map_state(
            MapStateDescriptor(
                "price_history_by_second",
                Types.INT(), # Chiave: il secondo (0, 10, 20, ...)
                Types.STRING() # Valore: lista di float serializzata come JSON
            )
        )

        # Carica il modello e lo scaler specifici per questo ticker
        try:
            model_path = os.path.join(MODEL_BASE_PATH, f"lstm_model_par2_{self.ticker}.h5")
            scaler_path = os.path.join(SCALER_BASE_PATH, f"scaler_par2_{self.ticker}.pkl")

            # Assicurati che i percorsi siano validi
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Modello non trovato: {model_path}")
            if not os.exists(scaler_path):
                raise FileNotFoundError(f"Scaler non trovato: {scaler_path}")

            self.model = load_model(model_path)
            self.scaler = joblib.load(scaler_path)
            logger.info(f"Modello e scaler caricati con successo per {self.ticker}")
        except Exception as e:
            logger.error(f"Errore durante il caricamento del modello/scaler per {self.ticker}: {e}")
            self.model = None
            self.scaler = None # Assicurati che siano None se il caricamento fallisce

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        """
        Elabora ogni record in ingresso dal topic Kafka.
        """
        # Se il modello o lo scaler non sono stati caricati, salta la predizione
        if self.model is None or self.scaler is None:
            logger.warning(f"Modello o scaler non caricati per {self.ticker}. Saltando la predizione.")
            return

        original_timestamp_ms = value['timestamp']
        current_price = value['price'] # Questo è il prezzo aggregato per l'ultimo minuto

        # Converti il timestamp Unix (ms) in un oggetto datetime
        current_dt = datetime.fromtimestamp(original_timestamp_ms / 1000)
        current_second = current_dt.second # Estrai il secondo (0, 10, 20, 30, 40, 50)

        # --- Gestione della cronologia dei prezzi per il secondo specifico ---
        # Recupera la cronologia esistente per questo specifico secondo
        history_json = self.price_history_by_second.get(current_second)
        if history_json:
            price_history = json.loads(history_json)
        else:
            price_history = []

        # Aggiungi il prezzo corrente alla cronologia
        price_history.append(current_price)

        # Mantieni solo gli ultimi N_LAG_STEPS valori
        if len(price_history) > N_LAG_STEPS:
            price_history = price_history[-N_LAG_STEPS:] # Prendi gli ultimi N_LAG_STEPS

        # Aggiorna lo stato con la nuova cronologia
        self.price_history_by_second.put(current_second, json.dumps(price_history))
        logger.debug(f"Ticker {self.ticker}, Secondo {current_second}: Cronologia aggiornata. Dimensione: {len(price_history)}")

        # --- Logica di Predizione ---
        # Esegui una predizione solo se abbiamo N_LAG_STEPS valori nella cronologia
        if len(price_history) == N_LAG_STEPS:
            # Prepara i dati per il modello LSTM (reshape a (1, N_LAG_STEPS, 1))
            # Il modello si aspetta (batch_size, timesteps, features)
            # Qui abbiamo 1 sample, N_LAG_STEPS timesteps, 1 feature (il prezzo)
            features = np.array(price_history).reshape(1, N_LAG_STEPS, 1)
            
            # Scala le feature usando lo scaler caricato
            # Lo scaler è stato addestrato su dati con una singola feature, quindi reshape a (n_samples, n_features) per transform
            scaled_features = self.scaler.transform(features.reshape(-1, 1)).reshape(1, N_LAG_STEPS, 1)

            try:
                # Esegui la predizione
                predicted_scaled_price = self.model.predict(scaled_features, verbose=0) # verbose=0 per sopprimere output di Keras
                # Inverti la trasformazione per ottenere il prezzo reale
                # Lo scaler è stato addestrato per de-scalare una singola feature
                predicted_price = self.scaler.inverse_transform(predicted_scaled_price).flatten()[0]

                # Calcola il target_timestamp (timestamp originale + 1 minuto)
                target_timestamp_dt = current_dt + timedelta(minutes=1)
                target_timestamp_ms = int(target_timestamp_dt.timestamp() * 1000)

                # Crea la riga di output
                output_row = Row(
                    target_timestamp=target_timestamp_ms,
                    predicted_price=float(predicted_price), # Assicurati che sia di tipo float
                    ticker=self.ticker
                )
                # Emetti la riga al topic di output
                ctx.output(output_row)
                logger.info(f"Ticker {self.ticker}, Secondo {current_second}: Prezzo previsto {predicted_price:.2f} per il target {target_timestamp_dt.strftime('%H:%M:%S')}")

            except Exception as e:
                logger.error(f"Errore durante la predizione per {self.ticker}, Secondo {current_second}: {e}")
        else:
            logger.debug(f"Ticker {self.ticker}, Secondo {current_second}: Cronologia incompleta ({len(price_history)}/{N_LAG_STEPS}) per la predizione a {current_dt.strftime('%H:%M:%S')}.")


def main():
    """
    Funzione principale per configurare ed eseguire il job Flink.
    """
    # Ottieni l'ambiente di esecuzione dello stream
    env = StreamExecutionEnvironment.get_execution_environment()
    # Imposta il parallelismo. Può essere sovrascritto durante l'invio del job.
    env.set_parallelism(1) # Esempio: 1 per test locale, aumenta per produzione
    # Abilita il checkpointing per la tolleranza ai guasti (ogni 60 secondi)
    env.enable_checkpointing(60000)

    # --- Configurazione della Sorgente Kafka ---
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("flink-prediction-consumer-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(INPUT_SCHEMA)
            .build()
        ) \
        .build()

    # Crea un DataStream dalla sorgente Kafka
    ds = env.from_source(kafka_source)

    # --- Chiave per Ticker e Applica la Logica di Predizione ---
    # `key_by` assicura che tutti i dati per un ticker specifico vadano allo stesso task Flink,
    # permettendo la gestione dello stato per ticker.
    predicted_stream = ds \
        .key_by(lambda x: x['ticker']) \
        .process(PredictionProcessFunction(), output_type=OUTPUT_SCHEMA)

    # --- Configurazione del Sink Kafka ---
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topic(OUTPUT_TOPIC) \
        .set_value_serialization_schema(
            JsonRowSerializationSchema.builder()
            .type_info(OUTPUT_SCHEMA)
            .build()
        ) \
        .build()

    # Invia lo stream delle predizioni al sink Kafka
    predicted_stream.sink_to(kafka_sink)

    logger.info("Avvio del job Flink di predizione in tempo reale...")
    # Esegui il job Flink
    env.execute("Real-time ML Prediction Job")

if __name__ == '__main__':
    main()