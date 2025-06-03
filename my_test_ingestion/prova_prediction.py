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
MODEL_SAVE_PATH = "model"
MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, "lstm_multi_ticker_prova.h5")
SCALER_FILENAME = os.path.join(MODEL_SAVE_PATH, "scaler.pkl")
TICKER_MAP_FILENAME = os.path.join(MODEL_SAVE_PATH, "ticker_map.json")

# N_STEPS must match the N_STEPS used in training (e.g., 30 for 30 minutes)
N_STEPS = 30 # Crucial: this must be the same value as in training

# Number of offset-based buffers (every 10 seconds, from :00 to :50)
NUM_OFFSET_BUFFERS = 6 # Corresponds to offsets :00, :10, :20, :30, :40, :50

# --- Kafka Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "aggregated_data"
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

    num_features = scaler.n_features_in_
    print(f"Number of features expected by the model: {num_features}")

except Exception as e:
    print(f"\u274C Error loading artifacts: {e}")
    sys.exit(1)

### Data Handling and Real-time Prediction

# `realtime_data_buffers` will now contain a list of `NUM_OFFSET_BUFFERS` lists for each ticker.
# Example: {'AAPL': [buffer_offset_00, buffer_offset_10, ..., buffer_offset_50]}
realtime_data_buffers = {}
# `last_timestamps` remains a single timestamp per ticker for order control
last_timestamps = {}

def get_ticker_code(ticker_name):
    """Returns the numeric code for a ticker name."""
    return ticker_name_to_code_map.get(ticker_name)

def make_prediction(ticker_name, new_data_timestamp, new_data_features):
    """
    Handles the arrival of new data and makes a prediction using the appropriate buffer.

    Args:
        ticker_name (str): The name of the ticker (e.g., 'AAPL').
        new_data_timestamp (datetime.datetime): The timestamp of the new aggregated data.
        new_data_features (np.array): The aggregated features (NOT scaled) for this timestamp.
    """
    global realtime_data_buffers, last_timestamps

    ticker_code = get_ticker_code(ticker_name)
    if ticker_code is None:
        print(f"\u274C Ticker '{ticker_name}' not found in mapping. Ignoring message.")
        return None

    # Initialize the `NUM_OFFSET_BUFFERS` buffers for the ticker if it's new
    if ticker_name not in realtime_data_buffers:
        realtime_data_buffers[ticker_name] = [[] for _ in range(NUM_OFFSET_BUFFERS)]
        last_timestamps[ticker_name] = None
        print(f"Initialized {NUM_OFFSET_BUFFERS} buffers for ticker: {ticker_name}")

    # Check if the timestamp is ahead (to avoid out-of-order or duplicate data)
    if last_timestamps[ticker_name] is not None and new_data_timestamp <= last_timestamps[ticker_name]:
        print(f"Warning: Old or duplicate data for {ticker_name} at {new_data_timestamp}. Ignoring.")
        return None

    # Apply the scaler to the new feature point
    try:
        scaled_features = scaler.transform(new_data_features.reshape(1, -1)).flatten()
    except ValueError as ve:
        print(f"\u274C Scaling error for {ticker_name} at {new_data_timestamp}: {ve}. Ensure features have the correct dimension ({num_features}).")
        return None
    except Exception as e:
        print(f"\u274C Unexpected error during scaling for {ticker_name} at {new_data_timestamp}: {e}")
        return None

    # Determine the buffer index based on the second of the timestamp
    # Example: 00s -> index 0, 10s -> index 1, ..., 50s -> index 5
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
        # `input_sequence` must be (1, N_STEPS, num_features)
        input_sequence = np.array(target_buffer).reshape(1, N_STEPS, num_features)
        
        # Prepare ticker input (1,)
        input_ticker_code = np.array([ticker_code], dtype=np.int32)

        # Perform the prediction
        try:
            prediction = model.predict([input_sequence, input_ticker_code], verbose=0)[0][0]
            print(f"Prediction for {ticker_name} (offset :{second_of_minute:02d}) at {new_data_timestamp}: {prediction:.4f}")
            return prediction
        except Exception as e:
            print(f"\u274C Error during prediction for {ticker_name} at {new_data_timestamp}: {e}")
            return None
    else:
        print(f"Buffer for {ticker_name} (offset :{second_of_minute:02d}) has {len(target_buffer)} points, needs {N_STEPS} to predict.")
        return None

if __name__ == "__main__":
    print("\n\U0001F535 Starting Kafka consumer for real-time predictions...")

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        #'group.id': KAFKA_GROUP_ID,
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
                    features_list = data.get('features')

                    if not all([ticker_name, timestamp_str, features_list is not None]):
                        print(f"\u274C Malformed Kafka message: {data}. Expected 'ticker', 'timestamp', 'features'.")
                        continue

                    # Convert timestamp to datetime object
                    new_data_timestamp = datetime.datetime.fromisoformat(timestamp_str)
                    
                    # Convert feature list to NumPy array
                    new_data_features = np.array(features_list, dtype=np.float32)

                    make_prediction(ticker_name, new_data_timestamp, new_data_features)

                except json.JSONDecodeError:
                    print(f"\u274C JSON decoding error from Kafka message: {msg.value()}")
                except Exception as e:
                    print(f"\u274C Error processing Kafka message: {e} - Message: {msg.value()}")

    except KeyboardInterrupt:
        print("\n\u2705 Keyboard interrupt. Closing Kafka consumer.")
    finally:
        consumer.close()
        print("Kafka consumer closed.")