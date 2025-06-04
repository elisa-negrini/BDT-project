

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







import os
import json
import numpy as np
import tensorflow as tf
import pickle
from datetime import datetime, timedelta
import logging
import time

# Importa KafkaConsumer dalla libreria kafka-python (è più adatta per ottenere i metadati dei topic)
# KafkaClient è di basso livello e non espone direttamente 'topics'
from kafka import KafkaConsumer, KafkaClient # Mantengo KafkaClient per il ping iniziale, ma useremo Consumer per i topics
from kafka.errors import NoBrokersAvailable, KafkaError # TopicError per errori specifici del topic

from pyflink.common import WatermarkStrategy, Duration, Row, RestartStrategies
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.checkpointing_mode import CheckpointingMode
import joblib
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common import Types


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base paths for models and scalers inside the Flink container
MODELS_BASE_PATH = "/opt/flink/models"
SCALERS_BASE_PATH = "/opt/flink/scalers"

# N_STEPS must match the N_STEPS used in training (e.g., 5 for 5 aggregated 10-second points)
N_STEPS = 5

FEATURE_COLUMNS_ORDERED = [
    "price_mean_1min", "price_mean_5min", "price_cv_5min", "price_mean_30min", "price_cv_30min",
    "size_tot_1min", "size_tot_5min", "size_tot_30min",
    "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
    "sentiment_news_mean_1day", "sentiment_news_mean_3days",
    "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
    "minutes_since_open", "day_of_week", "day_of_month", "week_of_year", "month_of_year",
    "market_open_spike_flag", "market_close_spike_flag", "eps", "free_cash_flow",
    "profit_margin", "debt_to_equity", "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"
]

def safe_float(x):
    """Safely converts a value to float, defaulting to 0.0 on error."""
    try:
        return float(x)
    except (ValueError, TypeError):
        return 0.0

class LSTMPredictionFunction(KeyedProcessFunction):
    """
    KeyedProcessFunction to handle real-time stock price predictions using
    per-ticker LSTM models loaded dynamically.
    """
    def open(self, runtime_context: RuntimeContext):
        descriptor = MapStateDescriptor(
            "buffer",
            Types.LONG(),
            Types.PICKLED_BYTE_ARRAY()
        )
        self.buffer_state = runtime_context.get_map_state(descriptor)
        logger.info("LSTMPredictionFunction opened successfully.")
        self.loaded_models = {}
        self.loaded_scalers = {}
        self.num_features_per_ticker = {}

    def _get_model_path(self, ticker_name):
        return os.path.join(MODELS_BASE_PATH, f"lstm_model_{ticker_name}.h5")

    def _get_scaler_path(self, ticker_name):
        return os.path.join(SCALERS_BASE_PATH, f"scaler_{ticker_name}.pkl")

    def _load_model_and_scaler_for_ticker(self, ticker_name):
        """
        Loads model and scaler for a given ticker, with caching.
        Returns (model, scaler, num_features) or (None, None, None) on failure.
        """
        if ticker_name not in self.loaded_models:
            model_path = self._get_model_path(ticker_name)
            scaler_path = self._get_scaler_path(ticker_name)
            try:
                model = tf.keras.models.load_model(model_path)
                with open(scaler_path, 'rb') as f:
                    scaler = joblib.load(f)
                
                self.loaded_models[ticker_name] = model
                self.loaded_scalers[ticker_name] = scaler
                self.num_features_per_ticker[ticker_name] = scaler.n_features_in_
                logger.info(f"\u2705 Loaded model and scaler for {ticker_name} from {model_path} and {scaler_path}. Features: {scaler.n_features_in_}")
            except Exception as e:
                logger.error(f"\u274C Error loading artifacts for {ticker_name}: {e}", exc_info=True)
                return None, None, None
        
        return (self.loaded_models.get(ticker_name), 
                self.loaded_scalers.get(ticker_name), 
                self.num_features_per_ticker.get(ticker_name))

    def process_element(self, value: Row, ctx: 'KeyedProcessFunction.Context'):
        try:
            ticker_name = value["ticker"]
            timestamp_str = value["timestamp"]
            timestamp_dt = datetime.fromisoformat(timestamp_str) 

            logger.debug(f"Processing element for ticker: {ticker_name}, timestamp: {timestamp_str}")

            model, scaler, num_features = self._load_model_and_scaler_for_ticker(ticker_name)
            if model is None or scaler is None:
                logger.warning(f"Skipping prediction for {ticker_name} due to missing model/scaler.")
                return

            current_features_for_scaling = []
            for col in FEATURE_COLUMNS_ORDERED:
                current_features_for_scaling.append(safe_float(value.get(col, 0.0)))
            
            current_features_np = np.array(current_features_for_scaling, dtype=np.float32)

            if current_features_np.shape[0] != num_features:
                logger.error(f"Feature count mismatch for {ticker_name} at {timestamp_str}. "
                             f"Expected {num_features}, got {current_features_np.shape[0]}. Skipping.")
                return

            # Note: For PyFlink MapState, the "key" of the MapState is implicitly the key_by value (ticker_name).
            # The map itself then stores your internal key-value pairs (timestamp_str -> features_list).
            # So, self.buffer_state.get() gets the map for the current ticker.
            current_ticker_buffer_map = self.buffer_state.get() or {}
            
            # Store the current unscaled feature list with its timestamp
            current_ticker_buffer_map[timestamp_str] = current_features_for_scaling
            
            # Sort the buffer by timestamp and keep only the latest N_STEPS elements
            sorted_buffer_items = sorted(current_ticker_buffer_map.items(), key=lambda item: item[0], reverse=True)[:N_STEPS]
            
            # Update the state with the pruned buffer. Convert back to dict if sorted_buffer_items is a list of tuples.
            self.buffer_state.put(dict(sorted_buffer_items))
            
            if len(sorted_buffer_items) == N_STEPS:
                logger.info(f"Buffer full ({N_STEPS} points) for ticker {ticker_name}. Performing prediction.")
                
                sequence_for_scaling = []
                # Re-sort in ascending order by timestamp for correct sequence for LSTM
                for ts_key, features_list in sorted(sorted_buffer_items, key=lambda item: item[0]):
                    sequence_for_scaling.append(features_list)
                
                features_np_sequence = np.array(sequence_for_scaling, dtype=np.float32)
                
                scaled_2d = scaler.transform(features_np_sequence.reshape(-1, num_features))
                
                scaled_input_lstm = scaled_2d.reshape(1, N_STEPS, num_features)
                
                logger.debug(f"Scaled LSTM input shape for {ticker_name}: {scaled_input_lstm.shape}")

                prediction = model.predict(scaled_input_lstm, verbose=0)[0][0]
                logger.info(f"Prediction for {ticker_name} (based on {timestamp_str}): {prediction}")

                predicted_for_timestamp = timestamp_dt + timedelta(seconds=60)
                
                prediction_output = {
                    "ticker": ticker_name,
                    "timestamp": predicted_for_timestamp.isoformat(timespec='seconds'),
                    "prediction": float(prediction)
                }

                yield Row(**prediction_output)
                
                self.buffer_state.clear()
                logger.debug(f"Buffer reset for ticker: {ticker_name}")

        except Exception as e:
            logger.error(f"Error processing element for key {ctx.current_key()}: {e}", exc_info=True)


def wait_for_kafka_topic(broker: str, topic: str, max_retries: int = 30, retry_delay: int = 5):
    """
    Attende che un topic Kafka sia disponibile e accessibile.
    """
    logger.info(f"⚙️  Tentativo di connessione a Kafka broker: {broker}")
    for i in range(1, max_retries + 1):
        try:
            # Usiamo KafkaConsumer perché ha un modo più diretto per ottenere i topic.
            # Il group_id non è strettamente necessario qui ma è richiesto dal costruttore.
            consumer = KafkaConsumer(
                bootstrap_servers=broker,
                client_id='flink-topic-checker',
                group_id=None, # Nessun gruppo consumer reale
                request_timeout_ms=5000, # Timeout per le richieste (e.g., fetching metadata)
            )
            
            # Forziamo un aggiornamento dei metadati
            consumer.poll(timeout_ms=1000)
            
            # Otteniamo i nomi dei topic dal consumer
            available_topics = consumer.topics() # Questo è il metodo corretto
            
            if topic in available_topics:
                logger.info(f"✅ Topic '{topic}' trovato su Kafka. Procedo.")
                consumer.close()
                return True
            else:
                logger.warning(f"⏳ Tentativo {i}/{max_retries}: Topic '{topic}' non ancora disponibile. Riprovo tra {retry_delay} secondi. Topics disponibili: {available_topics}")
                consumer.close()
                time.sleep(retry_delay)
        except NoBrokersAvailable:
            logger.warning(f"⏳ Tentativo {i}/{max_retries}: Nessun Kafka broker disponibile a {broker}. Riprovo tra {retry_delay} secondi...")
            time.sleep(retry_delay)
        except KafkaError as e:
            logger.warning(f"⏳ Tentativo {i}/{max_retries}: Errore Kafka generico ({e}). Riprovo tra {retry_delay} secondi...")
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"❌ Errore inatteso durante l'attesa del topic: {e}. Riprovo tra {retry_delay} secondi...", exc_info=True)
            time.sleep(retry_delay)
            
    logger.error(f"❌ ERRORE: Il topic '{topic}' non è stato trovato o il broker non è disponibile dopo {max_retries} tentativi.")
    return False

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(60000)
    env.get_checkpoint_config().set_checkpoint_timeout(300000)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
        restart_attempts=3,
        delay_between_attempts=timedelta(seconds=10)
    ))
    
    env.set_parallelism(1)

    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
    KAFKA_TOPIC_AGGREGATED = os.getenv("KAFKA_TOPIC_AGGREGATED", "aggregated_data")
    KAFKA_TOPIC_DASHBOARD = os.getenv("KAFKA_TOPIC_DASHBOARD", "prediction")

    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Kafka Input Topic: {KAFKA_TOPIC_AGGREGATED}")
    logger.info(f"Kafka Output Topic: {KAFKA_TOPIC_DASHBOARD}")

    # --- AGGIUNTA LOGICA DI RETRY QUI ---
    if not wait_for_kafka_topic(KAFKA_BROKER, KAFKA_TOPIC_AGGREGATED, max_retries=60, retry_delay=5):
        logger.error("Impossibile avviare il job Flink: il topic Kafka non è diventato disponibile.")
        # Potresti voler lanciare un'eccezione o uscire dal programma qui
        raise SystemExit("Topic Kafka non disponibile. Uscita.")
    # --- FINE LOGICA DI RETRY ---

    input_column_names = ["ticker", "timestamp"] + FEATURE_COLUMNS_ORDERED
    input_column_types = [Types.STRING(), Types.STRING()] + [Types.FLOAT()] * len(FEATURE_COLUMNS_ORDERED)

    kafka_deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW_NAMED(input_column_names, input_column_types)).build()

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(KAFKA_TOPIC_AGGREGATED) \
        .set_group_id("lstm_prediction_flink_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(kafka_deserialization_schema) \
        .build()

    data_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
        source_name="Kafka_Aggregated_Source"
    )

    producer_serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(
            Types.ROW_NAMED(
                ["ticker", "timestamp", "prediction"],
                [Types.STRING(), Types.STRING(), Types.FLOAT()]
            )
        ).build()

    kafka_producer = FlinkKafkaProducer(
        KAFKA_TOPIC_DASHBOARD,
        producer_serialization_schema,
        {'bootstrap.servers': KAFKA_BROKER}
    )

    predicted_stream = data_stream.key_by(lambda x: x["ticker"]).process(LSTMPredictionFunction())
    
    predicted_stream.print("Predictions Output")
    
    predicted_stream.add_sink(kafka_producer).name("Kafka_Dashboard_Sink")

    logger.info("Starting Flink job...")
    env.execute("LSTM Stock Price Prediction Job (Per-Ticker Models)")

if __name__ == "__main__":
    main()