import os
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import joblib
import json
import datetime
import time
from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import sys
import pytz

# Paths for saving artifacts
MODELS_BASE_PATH = "models_lstm/models"
SCALERS_BASE_PATH = "models_lstm/scalers"

TICKER_MAP_FILENAME = "models_lstm/ticker_map.json"

N_STEPS = 5

NUM_OFFSET_BUFFERS = 6

# ==== Market Opening Configuration ====
MARKET_TIMEZONE = pytz.timezone('US/Eastern')
MARKET_OPEN_TIME = datetime.time(9, 29)
MARKET_WARMUP_MINUTES = 5  # Do not make predictions for the first 5 minutes

# ==== Kafka Configuration ====
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC_INPUT = "aggregated_data"
KAFKA_TOPIC_OUTPUT = "prediction"
KAFKA_GROUP_ID = "prediction_consumer_group"

# ==== Cache for Models and Scalers ====
loaded_models = {}
loaded_scalers = {}
ticker_name_to_code_map = {}

KEY_FEATURE_TO_EMPHASIZE = "price_mean_1min"

# ==== Functions to get file paths ====
def get_model_path(ticker_name):
    """Constructs the path to a specific ticker's LSTM model file."""
    return os.path.join(MODELS_BASE_PATH, f"lstm_model_{ticker_name}.h5")

def get_scaler_path(ticker_name):
    """Constructs the path to a specific ticker's scaler file."""
    return os.path.join(SCALERS_BASE_PATH, f"scaler_{ticker_name}.pkl")

# ==== Function to check if we are in the market warmup period ====
def is_market_warmup_period(timestamp):
    """
    Checks if the given timestamp falls within the market warmup period.
    """
    if timestamp.tzinfo is None:
        market_time = MARKET_TIMEZONE.localize(timestamp)
    else:
        market_time = timestamp.astimezone(MARKET_TIMEZONE)

    # Check if it's a weekday (Monday=0, Sunday=6)
    if market_time.weekday() >= 5:
        return False

    market_open_datetime = market_time.replace(
        hour=MARKET_OPEN_TIME.hour,
        minute=MARKET_OPEN_TIME.minute,
        second=0,
        microsecond=0
    )

    warmup_end_datetime = market_open_datetime + datetime.timedelta(minutes=MARKET_WARMUP_MINUTES)

    is_warmup = market_open_datetime <= market_time < warmup_end_datetime

    if is_warmup:
        remaining_seconds = (warmup_end_datetime - market_time).total_seconds()
        print(f"Market warmup period active. No predictions until {warmup_end_datetime.strftime('%H:%M:%S')} "
              f"(remaining: {int(remaining_seconds)}s)")

    return is_warmup

def load_model_and_scaler_for_ticker(ticker_name):
    """
    Loads the model and scaler for a given ticker, with caching and reloading if the files have been modified on disk.
    """
    model_path = get_model_path(ticker_name)
    scaler_path = get_scaler_path(ticker_name)

    model_needs_reload = True
    scaler_needs_reload = True

    if ticker_name in loaded_models and os.path.exists(model_path):
        current_model_mtime = os.path.getmtime(model_path)
        if loaded_models[ticker_name].get('last_modified') == current_model_mtime:
            model_needs_reload = False
        else:
            print(f"Model file for {ticker_name} changed. Reloading.")

    if ticker_name in loaded_scalers and os.path.exists(scaler_path):
        current_scaler_mtime = os.path.getmtime(scaler_path)
        if loaded_scalers[ticker_name].get('last_modified') == current_scaler_mtime:
            scaler_needs_reload = False
        else:
            print(f"Scaler file for {ticker_name} changed. Reloading.")
            
    if model_needs_reload:
        try:
            model = tf.keras.models.load_model(model_path)
            loaded_models[ticker_name] = {'model': model, 'last_modified': os.path.getmtime(model_path)}
            print(f"\u2705 Loaded/Reloaded model for {ticker_name} from {model_path}")
        except Exception as e:
            print(f"\u274C Error loading model for {ticker_name} from {model_path}: {e}")
            return None, None, None

    if scaler_needs_reload:
        try:
            scaler = joblib.load(scaler_path)
            loaded_scalers[ticker_name] = {'scaler': scaler, 'last_modified': os.path.getmtime(scaler_path)}
            print(f"\u2705 Loaded/Reloaded scaler for {ticker_name} from {scaler_path}")
        except Exception as e:
            print(f"\u274C Error loading scaler for {ticker_name} from {scaler_path}: {e}")
            
            if ticker_name in loaded_models:
                del loaded_models[ticker_name]
            return None, None, None
            
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
            print(f"Loaded ticker map from {TICKER_MAP_FILENAME}: {ticker_name_to_code_map}")
    except FileNotFoundError:
        print(f"Ticker map file not found at {TICKER_MAP_FILENAME}. Ensure training script ran successfully and saved it.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error loading ticker map from {TICKER_MAP_FILENAME}: {e}", file=sys.stderr)
        sys.exit(1)

### Data Handling and Real-time Prediction

realtime_data_buffers = {}
last_timestamps = {}

def get_ticker_code(ticker_name):
    """Returns the numeric code for a ticker name from the loaded map."""
    return ticker_name_to_code_map.get(ticker_name)

def make_prediction(ticker_name, new_data_timestamp, new_data_features_dict, kafka_producer):
    """
    Handles the arrival of new data, processes it, and makes a prediction
    using the appropriate ticker-specific model and buffer.
    Sends the prediction to the dashboard Kafka topic.
    """
    global realtime_data_buffers, last_timestamps

    ticker_code = ticker_name_to_code_map.get(ticker_name)
    if ticker_code is None:
        print(f"Unknown ticker: {ticker_name}. No corresponding ticker code found. Skipping prediction.")
        return None

    model_for_ticker, scaler_for_ticker, num_features_for_ticker = load_model_and_scaler_for_ticker(ticker_name)
    if model_for_ticker is None or scaler_for_ticker is None:
        print(f"Cannot make prediction for {ticker_name} due to missing model/scaler.")
        return None

    # Initialize the NUM_OFFSET_BUFFERS buffers for the ticker if it's new
    if ticker_name not in realtime_data_buffers:
        realtime_data_buffers[ticker_name] = [[] for _ in range(NUM_OFFSET_BUFFERS)]
        last_timestamps[ticker_name] = None
        print(f"Initialized {NUM_OFFSET_BUFFERS} buffers for ticker: {ticker_name}")

    # Check if the timestamp is ahead (to avoid out-of-order or duplicate data)
    if last_timestamps[ticker_name] is not None and new_data_timestamp <= last_timestamps[ticker_name]:
        return None

    # ==== Feature Extraction and Ordering ====
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
        
    current_features_for_scaling = []
    for key in expected_feature_keys_in_order:
        value = new_data_features_dict.get(key)
        if value is None:
            print(f"Warning: Missing feature '{key}' for {ticker_name} at {new_data_timestamp}. Using 0.0.", file=sys.stderr)
            current_features_for_scaling.append(0.0)
        else:
            current_features_for_scaling.append(float(value))

    new_data_features_np = np.array(current_features_for_scaling, dtype=np.float32)

    # Verify the number of features matches what the current scaler expects
    if new_data_features_np.shape[0] != num_features_for_ticker:
        print(f"Feature mismatch for {ticker_name} at {new_data_timestamp}: "
              f"Expected {num_features_for_ticker} features, got {new_data_features_np.shape[0]}. "
              f"Please ensure the Flink aggregator output and the scaler's training data match feature sets and order.")
        return None

    # Apply the ticker-specific scaler to the new feature point
    try:
        scaled_features = scaler_for_ticker.transform(new_data_features_np.reshape(1, -1)).flatten()
    except ValueError as ve:
        print(f"Scaling error for {ticker_name} at {new_data_timestamp}: {ve}. Ensure features have the correct dimension ({num_features_for_ticker}).")
        return None
    except Exception as e:
        print(f"Unexpected error during scaling for {ticker_name} at {new_data_timestamp}: {e}")
        return None

    # Determine the buffer index based on the second of the timestamp
    second_of_minute = new_data_timestamp.second

    if second_of_minute % 10 != 0 or second_of_minute >= NUM_OFFSET_BUFFERS * 10:
        print(f"Warning: Timestamp {new_data_timestamp} (second: {second_of_minute:02d}) is not aligned to an expected 10-second offset (00, 10, ..., 50). Ignoring the point.")
        return None

    buffer_index = second_of_minute // 10

    current_ticker_buffers = realtime_data_buffers[ticker_name]
    target_buffer = current_ticker_buffers[buffer_index]

    # Always add data to the buffer, even during warmup
    target_buffer.append(scaled_features)

    # Remove the oldest data if the buffer exceeds N_STEPS
    if len(target_buffer) > N_STEPS:
        target_buffer.pop(0)

    last_timestamps[ticker_name] = new_data_timestamp

    if is_market_warmup_period(new_data_timestamp):
        print(f"Warmup period: Data received and buffered for {ticker_name} at {new_data_timestamp}, but no prediction made.")
        return None

    if len(target_buffer) == N_STEPS:
        input_sequence = np.array(target_buffer).reshape(1, N_STEPS, num_features_for_ticker)

        input_ticker_code = np.array([[ticker_code]], dtype=np.int32)

        model_inputs_list = [input_sequence, input_ticker_code]

        if KEY_FEATURE_TO_EMPHASIZE and key_feature_index != -1:
            input_key_feature_sequence = np.array(target_buffer)[:, key_feature_index:key_feature_index+1].reshape(1, N_STEPS, 1)
            model_inputs_list.append(input_key_feature_sequence)

        try:
            prediction = model_for_ticker.predict(model_inputs_list, verbose=0)[0][0]
            is_simulated = new_data_features_dict.get("is_simulated_prediction", False)

            predicted_for_timestamp = new_data_timestamp + datetime.timedelta(seconds=60)

            prediction_data = {
                "ticker": ticker_name,
                "timestamp": predicted_for_timestamp.isoformat(),
                "prediction": float(prediction),
                "is_simulated_prediction": is_simulated
            }

            try:
                kafka_producer.produce(KAFKA_TOPIC_OUTPUT, key=ticker_name, value=json.dumps(prediction_data).encode('utf-8'))
                kafka_producer.poll(0)
                print(f"Prediction for {ticker_name} (offset :{second_of_minute:02d}) at {new_data_timestamp} (Simulated: {is_simulated}): {prediction:.4f} \u27A1 Sent to '{KAFKA_TOPIC_OUTPUT}'")
            except Exception as kafka_e:
                print(f"Error sending message to Kafka dashboard topic: {kafka_e}")

            return prediction
        except Exception as e:
            print(f"Error during prediction for {ticker_name} at {new_data_timestamp}: {e}")
            return None
    else:
        return None

if __name__ == "__main__":
    print("\n Starting Kafka consumer for real-time predictions...")
    print(f"Market warmup configured: No predictions for {MARKET_WARMUP_MINUTES} minutes after {MARKET_OPEN_TIME.strftime('%H:%M')} EST")

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
    producer = Producer(producer_conf)

    try:
        consumer.subscribe([KAFKA_TOPIC_INPUT])
        print(f"Subscribed to Kafka topic: {KAFKA_TOPIC_INPUT}")
        print(f"Will publish predictions to Kafka topic: {KAFKA_TOPIC_OUTPUT}")

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

                    if not all([ticker_name, timestamp_str]):
                        print(f"Malformed Kafka message: {data}. Expected 'ticker' and 'timestamp'.")
                        continue

                    new_data_timestamp = datetime.datetime.fromisoformat(timestamp_str)

                    make_prediction(ticker_name, new_data_timestamp, data, producer)

                except json.JSONDecodeError:
                    print(f"JSON decoding error from Kafka message: {msg.value()}")
                except Exception as e:
                    print(f"Error processing Kafka message: {e} - Message: {msg.value()}")

    except KeyboardInterrupt:
        print("\n Keyboard interrupt. Closing Kafka consumer and producer.")
    finally:
        consumer.close() 
        producer.flush()
        print("Kafka consumer and producer closed.")