# #FUNZIONA SENZA FLINK




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
# #MODELS_BASE_PATH = "create_model_lstm/models"  # Cartella che contiene tutti i modelli
# MODELS_BASE_PATH = "create_model_lstm/models_2" 
# #SCALERS_BASE_PATH = "create_model_lstm/scalers" # Cartella che contiene tutti gli scalers
# SCALERS_BASE_PATH = "create_model_lstm/scalers_2"

# TICKER_MAP_FILENAME =  "create_model_lstm/ticker_map2.json"

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
# ticker_name_to_code_map = {} 

# #ticker_name_to_code_map = {"ABBV": 0, "PEP": 1, "MRK": 2, "NVDA": 3}
# #print(f"\u2705 Hardcoded ticker map loaded: {ticker_name_to_code_map}")


# KEY_FEATURE_TO_EMPHASIZE = "price_mean_1min"

# # --- Funzioni per ottenere i percorsi dei file ---
# def get_model_path(ticker_name):
#     #return os.path.join(MODELS_BASE_PATH, f"lstm_model_{ticker_name}.h5")
#     return os.path.join(MODELS_BASE_PATH, f"lstm_model2_{ticker_name}.h5")

# def get_scaler_path(ticker_name):
#     return os.path.join(SCALERS_BASE_PATH, f"scaler2_{ticker_name}.pkl")

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


# def load_ticker_map():
#     global ticker_name_to_code_map
#     print("provo a prendere il ticker_map")
#     try:
#         with open(TICKER_MAP_FILENAME, 'r') as f:
#             ticker_name_to_code_map = json.load(f)
#             print(f"\u2705 Loaded ticker map from {TICKER_MAP_FILENAME}: {ticker_name_to_code_map}")
#     except FileNotFoundError:
#         print(f"\u274C Ticker map file not found at {TICKER_MAP_FILENAME}. Ensure training script ran successfully and saved it.", file=sys.stderr)
#         sys.exit(1) # Exit if the map is not found, as it's crucial
#     except Exception as e:
#         print(f"\u274C Error loading ticker map from {TICKER_MAP_FILENAME}: {e}", file=sys.stderr)
#         sys.exit(1)


# ### Data Handling e Real-time Prediction

# # realtime_data_buffers will now contain a list of NUM_OFFSET_BUFFERS lists for each ticker.
# # Example: {'AAPL': [buffer_offset_00, buffer_offset_10, ..., buffer_offset_50]}
# realtime_data_buffers = {}
# # last_timestamps remains a single timestamp per ticker for order control
# last_timestamps = {}

# # Non useremo più get_ticker_code se ogni modello è per singolo ticker
# def get_ticker_code(ticker_name):
#     """Returns the numeric code for a ticker name."""
#     return ticker_name_to_code_map.get(ticker_name)

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

#     ticker_code = ticker_name_to_code_map.get(ticker_name)
#     if ticker_code is None:
#         print(f"\u274C Unknown ticker: {ticker_name}. No corresponding ticker code found. Skipping prediction.")
#         return None

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
#         "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
#         "sentiment_news_mean_1day", "sentiment_news_mean_3days",
#         "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
#         "minutes_since_open", "day_of_week", "day_of_month", "week_of_year", "month_of_year",
#         "market_open_spike_flag", "market_close_spike_flag",
#         "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
#         "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"
#     ]


#     feature_cols_for_this_ticker = [key for key in expected_feature_keys_in_order if key != 'y'] # Assuming 'y' is never in input features
    
#     key_feature_index = -1
#     if KEY_FEATURE_TO_EMPHASIZE and KEY_FEATURE_TO_EMPHASIZE in feature_cols_for_this_ticker:
#         key_feature_index = feature_cols_for_this_ticker.index(KEY_FEATURE_TO_EMPHASIZE)
#         # print(f"[{ticker_name}] Key feature '{KEY_FEATURE_TO_EMPHASIZE}' found at index {key_feature_index}")
    
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

#         # Prepare the ticker code input (shape: (1, 1))
#         input_ticker_code = np.array([[ticker_code]], dtype=np.int32)

#         # Start with the two mandatory inputs
#         model_inputs_list = [input_sequence, input_ticker_code]


#         # Prepare the key feature sequence input if applicable
#         if KEY_FEATURE_TO_EMPHASIZE and key_feature_index != -1:
#             # Extract the key feature from the buffer, reshape it to (1, N_STEPS, 1)
#             input_key_feature_sequence = np.array(target_buffer)[:, key_feature_index:key_feature_index+1].reshape(1, N_STEPS, 1)
#             model_inputs_list.append(input_key_feature_sequence)
            
#         # Perform the prediction using the *ticker-specific* model
#         try:
#             # Rimuovi l'input del ticker code se il modello è per singolo ticker
#             prediction = model_for_ticker.predict(model_inputs_list, verbose=0)[0][0]
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

#     load_ticker_map()

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














































# #FUNZIONA SENZA FLINK CON BLOCCO 9.30-35




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
# import pytz

# # Paths for saving artifacts
# #MODELS_BASE_PATH = "create_model_lstm/models"  # Cartella che contiene tutti i modelli
# MODELS_BASE_PATH = "create_model_lstm/models_2" 
# #SCALERS_BASE_PATH = "create_model_lstm/scalers" # Cartella che contiene tutti gli scalers
# SCALERS_BASE_PATH = "create_model_lstm/scalers_2"

# TICKER_MAP_FILENAME =  "create_model_lstm/ticker_map2.json"

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
# ticker_name_to_code_map = {} 

# #ticker_name_to_code_map = {"ABBV": 0, "PEP": 1, "MRK": 2, "NVDA": 3}
# #print(f"\u2705 Hardcoded ticker map loaded: {ticker_name_to_code_map}")


# KEY_FEATURE_TO_EMPHASIZE = "price_mean_1min"

# # Definizione del fuso orario di New York (Eastern Time)
# ny_timezone = pytz.timezone('America/New_York')

# # --- Funzione per controllare se il mercato è aperto ---
# def is_market_open(check_time: datetime.datetime) -> bool:
#     """
#     Controlla se il mercato azionario USA è aperto in base all'ora fornita.
#     L'ora fornita deve essere un oggetto datetime consapevole del fuso orario.
#     """
#     # Converti l'ora fornita al fuso orario di New York per consistenza
#     time_in_ny = check_time.astimezone(ny_timezone)

#     market_open_time = time_in_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#     market_close_time = time_in_ny.replace(hour=16, minute=0, second=0, microsecond=0)

#     # Check if it's a weekday (0=Monday, 6=Sunday)
#     if time_in_ny.weekday() >= 5: # Saturday or Sunday
#         return False

#     return market_open_time <= time_in_ny < market_close_time


# # --- Funzioni per ottenere i percorsi dei file ---
# def get_model_path(ticker_name):
#     #return os.path.join(MODELS_BASE_PATH, f"lstm_model_{ticker_name}.h5")
#     return os.path.join(MODELS_BASE_PATH, f"lstm_model2_{ticker_name}.h5")

# def get_scaler_path(ticker_name):
#     return os.path.join(SCALERS_BASE_PATH, f"scaler2_{ticker_name}.pkl")

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


# def load_ticker_map():
#     global ticker_name_to_code_map
#     print("provo a prendere il ticker_map")
#     try:
#         with open(TICKER_MAP_FILENAME, 'r') as f:
#             ticker_name_to_code_map = json.load(f)
#             print(f"\u2705 Loaded ticker map from {TICKER_MAP_FILENAME}: {ticker_name_to_code_map}")
#     except FileNotFoundError:
#         print(f"\u274C Ticker map file not found at {TICKER_MAP_FILENAME}. Ensure training script ran successfully and saved it.", file=sys.stderr)
#         sys.exit(1) # Exit if the map is not found, as it's crucial
#     except Exception as e:
#         print(f"\u274C Error loading ticker map from {TICKER_MAP_FILENAME}: {e}", file=sys.stderr)
#         sys.exit(1)


# ### Data Handling e Real-time Prediction

# # realtime_data_buffers will now contain a list of NUM_OFFSET_BUFFERS lists for each ticker.
# # Example: {'AAPL': [buffer_offset_00, buffer_offset_10, ..., buffer_offset_50]}
# realtime_data_buffers = {}
# # last_timestamps remains a single timestamp per ticker for order control
# last_timestamps = {}
# last_market_status = {}

# # Non useremo più get_ticker_code se ogni modello è per singolo ticker
# def get_ticker_code(ticker_name):
#     """Returns the numeric code for a ticker name."""
#     return ticker_name_to_code_map.get(ticker_name)

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
#     global realtime_data_buffers, last_timestamps, last_market_status

#     if new_data_timestamp.tzinfo is None:
#         new_data_timestamp = new_data_timestamp.replace(tzinfo=pytz.utc)

#     ticker_code = ticker_name_to_code_map.get(ticker_name)
#     if ticker_code is None:
#         print(f"\u274C Unknown ticker: {ticker_name}. No corresponding ticker code found. Skipping prediction.")
#         return None

#     # Carica dinamicamente il modello, lo scaler e num_features per questo ticker
#     model_for_ticker, scaler_for_ticker, num_features_for_ticker = load_model_and_scaler_for_ticker(ticker_name)
#     if model_for_ticker is None or scaler_for_ticker is None:
#         print(f"\u274C Cannot make prediction for {ticker_name} due to missing model/scaler.")
#         return None

#     # Initialize the NUM_OFFSET_BUFFERS buffers for the ticker if it's new
#     if ticker_name not in realtime_data_buffers:
#         realtime_data_buffers[ticker_name] = [[] for _ in range(NUM_OFFSET_BUFFERS)]
#         last_timestamps[ticker_name] = None
#         last_market_status[ticker_name] = False 
#         print(f"Initialized {NUM_OFFSET_BUFFERS} buffers for ticker: {ticker_name}")

    
#     current_market_is_open = is_market_open(new_data_timestamp)


#     if not last_market_status[ticker_name] and current_market_is_open:
#         print(f"\u2705 Market just opened for {ticker_name} at {new_data_timestamp}. Resetting all data buffers.")
#         realtime_data_buffers[ticker_name] = [[] for _ in range(NUM_OFFSET_BUFFERS)]
#         # Importante: resetta anche l'ultimo timestamp per non bloccare i primi dati reali
#         last_timestamps[ticker_name] = None 

#     last_market_status[ticker_name] = current_market_is_open

#     # Check if the timestamp is ahead (to avoid out-of-order or duplicate data)
#     if last_timestamps[ticker_name] is not None and new_data_timestamp <= last_timestamps[ticker_name]:
#         # print(f"Warning: Old or duplicate data for {ticker_name} at {new_data_timestamp}. Ignoring.")
#         return None

#     # --- Feature Extraction and Ordering ---
#     expected_feature_keys_in_order = [
#         "price_mean_1min", "price_mean_5min", "price_std_5min", "price_mean_30min", "price_std_30min",
#         "size_tot_1min", "size_tot_5min", "size_tot_30min",
#         "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
#         "sentiment_news_mean_1day", "sentiment_news_mean_3days",
#         "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
#         "minutes_since_open", "day_of_week", "day_of_month", "week_of_year", "month_of_year",
#         "market_open_spike_flag", "market_close_spike_flag",
#         "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
#         "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"
#     ]


#     feature_cols_for_this_ticker = [key for key in expected_feature_keys_in_order if key != 'y'] # Assuming 'y' is never in input features
    
#     key_feature_index = -1
#     if KEY_FEATURE_TO_EMPHASIZE and KEY_FEATURE_TO_EMPHASIZE in feature_cols_for_this_ticker:
#         key_feature_index = feature_cols_for_this_ticker.index(KEY_FEATURE_TO_EMPHASIZE)
#         # print(f"[{ticker_name}] Key feature '{KEY_FEATURE_TO_EMPHASIZE}' found at index {key_feature_index}")
    
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

#         # Prepare the ticker code input (shape: (1, 1))
#         input_ticker_code = np.array([[ticker_code]], dtype=np.int32)

#         # Start with the two mandatory inputs
#         model_inputs_list = [input_sequence, input_ticker_code]


#         # Prepare the key feature sequence input if applicable
#         if KEY_FEATURE_TO_EMPHASIZE and key_feature_index != -1:
#             # Extract the key feature from the buffer, reshape it to (1, N_STEPS, 1)
#             input_key_feature_sequence = np.array(target_buffer)[:, key_feature_index:key_feature_index+1].reshape(1, N_STEPS, 1)
#             model_inputs_list.append(input_key_feature_sequence)
            
#         # Perform the prediction using the *ticker-specific* model
#         try:
#             # Rimuovi l'input del ticker code se il modello è per singolo ticker
#             prediction = model_for_ticker.predict(model_inputs_list, verbose=0)[0][0]
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

#     load_ticker_map()

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



































#FUNZIONA SENZA FLINK

import os
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import joblib
import json
import datetime
import time
from confluent_kafka import Consumer, KafkaException, KafkaError, Producer # Importa Producer
import sys
import pytz

# Paths for saving artifacts
#MODELS_BASE_PATH = "create_model_lstm/models"  # Cartella che contiene tutti i modelli
MODELS_BASE_PATH = "models_lstm/models" 
#SCALERS_BASE_PATH = "create_model_lstm/scalers" # Cartella che contiene tutti gli scalers
SCALERS_BASE_PATH = "models_lstm/scalers"

TICKER_MAP_FILENAME =  "models_lstm/ticker_map.json"

# TICKER_MAP_FILENAME rimane, ma il suo uso potrebbe cambiare se ogni modello ha il suo input specifico
# TICKER_MAP_FILENAME = os.path.join(MODEL_SAVE_PATH, "ticker_map.json") # Potrebbe non servire se i modelli sono per singolo ticker

# N_STEPS must match the N_STEPS used in training (e.g., 5 for 5 aggregated 10-second points)
N_STEPS = 5 # Crucial: this must be the same value as in training

# Number of offset-based buffers (every 10 seconds, from :00 to :50)
NUM_OFFSET_BUFFERS = 6 # Corresponds to offsets :00, :10, :20, :30, :40, :50

# --- Market Opening Configuration ---
# Definisci il timezone EST/EDT per il mercato americano
MARKET_TIMEZONE = pytz.timezone('US/Eastern')
MARKET_OPEN_TIME = datetime.time(9, 29)  # 9:30 AM
MARKET_WARMUP_MINUTES = 5  # Non fare prediction per i primi 5 minuti

# --- Kafka Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC_INPUT = "aggregated_data" # Topic da cui leggiamo i dati aggregati
KAFKA_TOPIC_OUTPUT = "prediction"     # Nuovo topic per inviare le previsioni
KAFKA_GROUP_ID = "prediction_consumer_group"

# --- Cache per Modelli e Scalers ---
loaded_models = {}
loaded_scalers = {}
ticker_name_to_code_map = {} 

#ticker_name_to_code_map = {"ABBV": 0, "PEP": 1, "MRK": 2, "NVDA": 3}
#print(f"\u2705 Hardcoded ticker map loaded: {ticker_name_to_code_map}")

KEY_FEATURE_TO_EMPHASIZE = "price_mean_1min"

# --- Funzioni per ottenere i percorsi dei file ---
def get_model_path(ticker_name):
    #return os.path.join(MODELS_BASE_PATH, f"lstm_model_{ticker_name}.h5")
    return os.path.join(MODELS_BASE_PATH, f"lstm_model_{ticker_name}.h5")

def get_scaler_path(ticker_name):
    return os.path.join(SCALERS_BASE_PATH, f"scaler_{ticker_name}.pkl")

# --- Funzione per verificare se siamo nel periodo di warmup del mercato ---
def is_market_warmup_period(timestamp):
    """
    Verifica se il timestamp è nel periodo di warmup del mercato (primi 5 minuti dopo l'apertura).
    
    Args:
        timestamp (datetime.datetime): Il timestamp da verificare
        
    Returns:
        bool: True se siamo nel periodo di warmup, False altrimenti
    """
    # Converti il timestamp al timezone del mercato se non è già localizzato
    if timestamp.tzinfo is None:
        # Assumiamo che timestamp senza timezone sia già in orario del mercato
        market_time = MARKET_TIMEZONE.localize(timestamp)
    else:
        market_time = timestamp.astimezone(MARKET_TIMEZONE)
    
    # Controlla se siamo in un giorno lavorativo (lunedì=0, domenica=6)
    if market_time.weekday() >= 5:  # Sabato o domenica
        return False
    
    # Calcola l'orario di apertura del mercato per questo giorno
    market_open_datetime = market_time.replace(
        hour=MARKET_OPEN_TIME.hour,
        minute=MARKET_OPEN_TIME.minute,
        second=0,
        microsecond=0
    )
    
    # Calcola l'orario di fine warmup (5 minuti dopo l'apertura)
    warmup_end_datetime = market_open_datetime + datetime.timedelta(minutes=MARKET_WARMUP_MINUTES)
    
    # Verifica se siamo nel periodo di warmup
    is_warmup = market_open_datetime <= market_time < warmup_end_datetime
    
    if is_warmup:
        remaining_seconds = (warmup_end_datetime - market_time).total_seconds()
        print(f"\U0001F7E1 Market warmup period active. No predictions until {warmup_end_datetime.strftime('%H:%M:%S')} "
              f"(remaining: {int(remaining_seconds)}s)")
    
    return is_warmup

# --- Funzione per caricare dinamicamente Modelli e Scalers ---
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

# def load_ticker_map():
#     global ticker_name_to_code_map
#     print("provo a prendere il ticker_map")
#     try:
#         with open(TICKER_MAP_FILENAME, 'r') as f:
#             ticker_name_to_code_map = json.load(f)
#             print(f"\u2705 Loaded ticker map from {TICKER_MAP_FILENAME}: {ticker_name_to_code_map}")
#     except FileNotFoundError:
#         print(f"\u274C Ticker map file not found at {TICKER_MAP_FILENAME}. Ensure training script ran successfully and saved it.", file=sys.stderr)
#         sys.exit(1) # Exit if the map is not found, as it's crucial
#     except Exception as e:
#         print(f"\u274C Error loading ticker map from {TICKER_MAP_FILENAME}: {e}", file=sys.stderr)
#         sys.exit(1)





# --- Funzione per caricare dinamicamente Modelli e Scalers (con ricaricamento) ---
def load_model_and_scaler_for_ticker(ticker_name):
    """
    Carica il modello e lo scaler per un dato ticker, con caching e ricaricamento
    se i file sono stati modificati sul disco.
    """
    model_path = get_model_path(ticker_name)
    scaler_path = get_scaler_path(ticker_name)

    model_needs_reload = True
    scaler_needs_reload = True

    # Controlla se il modello è già in cache e se il file è stato modificato
    if ticker_name in loaded_models and os.path.exists(model_path):
        current_model_mtime = os.path.getmtime(model_path)
        if loaded_models[ticker_name].get('last_modified') == current_model_mtime:
            model_needs_reload = False
        else:
            print(f"\u21BA Model file for {ticker_name} changed. Reloading.")

    # Controlla se lo scaler è già in cache e se il file è stato modificato
    if ticker_name in loaded_scalers and os.path.exists(scaler_path):
        current_scaler_mtime = os.path.getmtime(scaler_path)
        if loaded_scalers[ticker_name].get('last_modified') == current_scaler_mtime:
            scaler_needs_reload = False
        else:
            print(f"\u21BA Scaler file for {ticker_name} changed. Reloading.")
            
    # Se il modello non è in cache O ha bisogno di essere ricaricato
    if model_needs_reload:
        try:
            model = tf.keras.models.load_model(model_path)
            loaded_models[ticker_name] = {'model': model, 'last_modified': os.path.getmtime(model_path)}
            print(f"\u2705 Loaded/Reloaded model for {ticker_name} from {model_path}")
        except Exception as e:
            print(f"\u274C Error loading model for {ticker_name} from {model_path}: {e}")
            return None, None, None

    # Se lo scaler non è in cache O ha bisogno di essere ricaricato
    if scaler_needs_reload:
        try:
            scaler = joblib.load(scaler_path)
            loaded_scalers[ticker_name] = {'scaler': scaler, 'last_modified': os.path.getmtime(scaler_path)}
            print(f"\u2705 Loaded/Reloaded scaler for {ticker_name} from {scaler_path}")
        except Exception as e:
            print(f"\u274C Error loading scaler for {ticker_name} from {scaler_path}: {e}")
            # Se lo scaler fallisce, invalidiamo anche il modello perché non possiamo fare prediction senza scaler
            if ticker_name in loaded_models:
                del loaded_models[ticker_name]
            return None, None, None
            
    # Recupera il modello e lo scaler dalla cache
    model_obj = loaded_models[ticker_name]['model']
    scaler_obj = loaded_scalers[ticker_name]['scaler']
    num_features = scaler_obj.n_features_in_
    
    return model_obj, scaler_obj, num_features

def load_ticker_map():
    global ticker_name_to_code_map
    print(f"Attempting to load ticker_map from {TICKER_MAP_FILENAME}")
    try:
        with open(TICKER_MAP_FILENAME, 'r') as f:
            ticker_name_to_code_map = json.load(f)
            print(f"\u2705 Loaded ticker map from {TICKER_MAP_FILENAME}: {ticker_name_to_code_map}")
    except FileNotFoundError:
        print(f"\u274C Ticker map file not found at {TICKER_MAP_FILENAME}. Ensure training script ran successfully and saved it.", file=sys.stderr)
        sys.exit(1) # Exit if the map is not found, as it's crucial
    except Exception as e:
        print(f"\u274C Error loading ticker map from {TICKER_MAP_FILENAME}: {e}", file=sys.stderr)
        sys.exit(1)

### Data Handling e Real-time Prediction

# realtime_data_buffers will now contain a list of NUM_OFFSET_BUFFERS lists for each ticker.
# Example: {'AAPL': [buffer_offset_00, buffer_offset_10, ..., buffer_offset_50]}
realtime_data_buffers = {}
# last_timestamps remains a single timestamp per ticker for order control
last_timestamps = {}

# Non useremo più get_ticker_code se ogni modello è per singolo ticker
def get_ticker_code(ticker_name):
    """Returns the numeric code for a ticker name."""
    return ticker_name_to_code_map.get(ticker_name)

def make_prediction(ticker_name, new_data_timestamp, new_data_features_dict, kafka_producer):
    """
    Handles the arrival of new data and makes a prediction using the appropriate buffer.
    Sends the prediction to the dashboard Kafka topic.

    Args:
        ticker_name (str): The name of the ticker (e.g., 'AAPL').
        new_data_timestamp (datetime.datetime): The timestamp of the new aggregated data.
        new_data_features_dict (dict): The dictionary of aggregated features (NOT scaled) for this timestamp,
                                       as produced by the Flink aggregator.
        kafka_producer (Producer): The Kafka producer instance to send messages.
    """
    global realtime_data_buffers, last_timestamps

    ticker_code = ticker_name_to_code_map.get(ticker_name)
    if ticker_code is None:
        print(f"\u274C Unknown ticker: {ticker_name}. No corresponding ticker code found. Skipping prediction.")
        return None

    # Carica dinamicamente il modello, lo scaler e num_features per questo ticker
    model_for_ticker, scaler_for_ticker, num_features_for_ticker = load_model_and_scaler_for_ticker(ticker_name)
    if model_for_ticker is None or scaler_for_ticker is None:
        print(f"\u274C Cannot make prediction for {ticker_name} due to missing model/scaler.")
        return None

    # Initialize the NUM_OFFSET_BUFFERS buffers for the ticker if it's new
    if ticker_name not in realtime_data_buffers:
        realtime_data_buffers[ticker_name] = [[] for _ in range(NUM_OFFSET_BUFFERS)]
        last_timestamps[ticker_name] = None
        print(f"Initialized {NUM_OFFSET_BUFFERS} buffers for ticker: {ticker_name}")

    # Check if the timestamp is ahead (to avoid out-of-order or duplicate data)
    if last_timestamps[ticker_name] is not None and new_data_timestamp <= last_timestamps[ticker_name]:
        # print(f"Warning: Old or duplicate data for {ticker_name} at {new_data_timestamp}. Ignoring.")
        return None

    # --- Feature Extraction and Ordering ---
    expected_feature_keys_in_order = [
        "price_mean_1min", "price_mean_5min", "price_std_5min", "price_mean_30min", "price_std_30min",
        "size_tot_1min", "size_tot_5min", "size_tot_30min",
        "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
        "sentiment_news_mean_1day", "sentiment_news_mean_3days",
        "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
        "minutes_since_open", "day_of_week", "day_of_month", "week_of_year", "month_of_year",
        "market_open_spike_flag", "market_close_spike_flag",
        "eps", "free_cash_flow", "profit_margin", "debt_to_equity",
        "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment"
    ]

    feature_cols_for_this_ticker = [key for key in expected_feature_keys_in_order if key != 'y'] # Assuming 'y' is never in input features
    
    key_feature_index = -1
    if KEY_FEATURE_TO_EMPHASIZE and KEY_FEATURE_TO_EMPHASIZE in feature_cols_for_this_ticker:
        key_feature_index = feature_cols_for_this_ticker.index(KEY_FEATURE_TO_EMPHASIZE)
        # print(f"[{ticker_name}] Key feature '{KEY_FEATURE_TO_EMPHASIZE}' found at index {key_feature_index}")
    
    current_features_for_scaling = []
    for key in expected_feature_keys_in_order:
        value = new_data_features_dict.get(key)
        if value is None:
            print(f"\u274C Warning: Missing feature '{key}' for {ticker_name} at {new_data_timestamp}. Using 0.0.", file=sys.stderr)
            current_features_for_scaling.append(0.0)
        else:
            current_features_for_scaling.append(float(value))

    new_data_features_np = np.array(current_features_for_scaling, dtype=np.float32)

    # Verify the number of features matches what the *current scaler* expects
    if new_data_features_np.shape[0] != num_features_for_ticker:
        print(f"\u274C Feature mismatch for {ticker_name} at {new_data_timestamp}: "
              f"Expected {num_features_for_ticker} features, got {new_data_features_np.shape[0]}. "
              f"Please ensure the Flink aggregator output and the scaler's training data match feature sets and order.")
        return None

    # Apply the *ticker-specific* scaler to the new feature point
    try:
        scaled_features = scaler_for_ticker.transform(new_data_features_np.reshape(1, -1)).flatten()
    except ValueError as ve:
        print(f"\u274C Scaling error for {ticker_name} at {new_data_timestamp}: {ve}. Ensure features have the correct dimension ({num_features_for_ticker}).")
        return None
    except Exception as e:
        print(f"\u274C Unexpected error during scaling for {ticker_name} at {new_data_timestamp}: {e}")
        return None

    # Determine the buffer index based on the second of the timestamp
    second_of_minute = new_data_timestamp.second
    
    if second_of_minute % 10 != 0 or second_of_minute >= NUM_OFFSET_BUFFERS * 10:
        print(f"\u274C Warning: Timestamp {new_data_timestamp} (second: {second_of_minute:02d}) is not aligned to an expected 10-second offset (00, 10, ..., 50). Ignoring the point.")
        return None
    
    buffer_index = second_of_minute // 10

    current_ticker_buffers = realtime_data_buffers[ticker_name]
    target_buffer = current_ticker_buffers[buffer_index]
    
    # SEMPRE aggiungi i dati al buffer, anche durante il warmup
    target_buffer.append(scaled_features)

    # Remove the oldest data if the buffer exceeds N_STEPS
    if len(target_buffer) > N_STEPS:
        target_buffer.pop(0)

    last_timestamps[ticker_name] = new_data_timestamp

    # *** CONTROLLO PERIODO DI WARMUP ***
    # Verifica se siamo nel periodo di warmup del mercato
    if is_market_warmup_period(new_data_timestamp):
        # Durante il warmup, ricevi e salva i dati ma non fare prediction
        print(f"\U0001F7E1 Warmup period: Data received and buffered for {ticker_name} at {new_data_timestamp}, but no prediction made.")
        return None

    # Check if the specific buffer has enough data to predict
    if len(target_buffer) == N_STEPS:
        input_sequence = np.array(target_buffer).reshape(1, N_STEPS, num_features_for_ticker)

        # Prepare the ticker code input (shape: (1, 1))
        input_ticker_code = np.array([[ticker_code]], dtype=np.int32)

        # Start with the two mandatory inputs
        model_inputs_list = [input_sequence, input_ticker_code]

        # Prepare the key feature sequence input if applicable
        if KEY_FEATURE_TO_EMPHASIZE and key_feature_index != -1:
            # Extract the key feature from the buffer, reshape it to (1, N_STEPS, 1)
            input_key_feature_sequence = np.array(target_buffer)[:, key_feature_index:key_feature_index+1].reshape(1, N_STEPS, 1)
            model_inputs_list.append(input_key_feature_sequence)
            
        # Perform the prediction using the *ticker-specific* model
        try:
            # Rimuovi l'input del ticker code se il modello è per singolo ticker
            prediction = model_for_ticker.predict(model_inputs_list, verbose=0)[0][0]
            is_simulated = new_data_features_dict.get("is_simulated_prediction", False)
            
            predicted_for_timestamp = new_data_timestamp + datetime.timedelta(seconds=60)

            prediction_data = {
                "ticker": ticker_name,
                "timestamp": predicted_for_timestamp.isoformat(),
                "prediction": float(prediction), # Assicurati che sia un float serializzabile
                "is_simulated_prediction": is_simulated
            }
            
            # Send prediction to Kafka dashboard topic
            try:
                kafka_producer.produce(KAFKA_TOPIC_OUTPUT, key=ticker_name, value=json.dumps(prediction_data).encode('utf-8'))
                kafka_producer.poll(0) # Poll per gestire i callback asincroni (potrebbe non essere strettamente necessario qui)
                print(f"\U0001F7E2 Prediction for {ticker_name} (offset :{second_of_minute:02d}) at {new_data_timestamp} (Simulated: {is_simulated}): {prediction:.4f} \u27A1 Sent to '{KAFKA_TOPIC_OUTPUT}'")
            except Exception as kafka_e:
                print(f"\u274C Error sending message to Kafka dashboard topic: {kafka_e}")

            return prediction
        except Exception as e:
            print(f"\u274C Error during prediction for {ticker_name} at {new_data_timestamp}: {e}")
            return None
    else:
        # print(f"Buffer for {ticker_name} (offset :{second_of_minute:02d}) has {len(target_buffer)} points, needs {N_STEPS} to predict.")
        return None

if __name__ == "__main__":
    print("\n\U0001F535 Starting Kafka consumer for real-time predictions...")
    print(f"\U0001F7E1 Market warmup configured: No predictions for {MARKET_WARMUP_MINUTES} minutes after {MARKET_OPEN_TIME.strftime('%H:%M')} EST")

    load_ticker_map()

    # Kafka Consumer Configuration
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'latest'
    }

    # Kafka Producer Configuration
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER
    }

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf) # Inizializza il producer

    try:
        consumer.subscribe([KAFKA_TOPIC_INPUT])
        print(f"\u2705 Subscribed to Kafka topic: {KAFKA_TOPIC_INPUT}")
        print(f"\u2705 Will publish predictions to Kafka topic: {KAFKA_TOPIC_OUTPUT}")

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
                    # Pass the whole dict to make_prediction to extract all features and the flag
                    
                    if not all([ticker_name, timestamp_str]): # features_dict is now derived inside make_prediction
                        print(f"\u274C Malformed Kafka message: {data}. Expected 'ticker' and 'timestamp'.")
                        continue

                    # Convert timestamp to datetime object
                    new_data_timestamp = datetime.datetime.fromisoformat(timestamp_str)
                    
                    # Pass the producer instance to make_prediction
                    make_prediction(ticker_name, new_data_timestamp, data, producer)

                except json.JSONDecodeError:
                    print(f"\u274C JSON decoding error from Kafka message: {msg.value()}")
                except Exception as e:
                    print(f"\u274C Error processing Kafka message: {e} - Message: {msg.value()}")

    except KeyboardInterrupt:
        print("\n\u2705 Keyboard interrupt. Closing Kafka consumer and producer.")
    finally:
        consumer.close()
        producer.flush() # Assicurati che tutti i messaggi in coda siano inviati prima di chiudere
        print("Kafka consumer and producer closed.")

















