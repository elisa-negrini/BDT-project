# import os
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from tensorflow.keras import layers, models
# from sklearn.model_selection import train_test_split
# from sqlalchemy import create_engine, text
# import psycopg2
# import time
# from psycopg2 import OperationalError
# from sklearn.preprocessing import MinMaxScaler
# import joblib # Import to save/load the scaler
# import sys
# import multiprocessing # Import for parallelization
# import json # To save the ticker_map

# # --- GPU Configuration (if applicable) ---
# num_cpu_cores = os.cpu_count()
# if num_cpu_cores:
#     tf.config.threading.set_inter_op_parallelism_threads(1) # Generally 1 to avoid competition among inter-op threads of a single process
#     tf.config.threading.set_intra_op_parallelism_threads(num_cpu_cores // multiprocessing.cpu_count() if multiprocessing.cpu_count() > 0 else 1) # Divide cores among processes if useful
#     print(f"\u2705 TensorFlow configured for CPU parallelism with {num_cpu_cores} cores detected.")
# else:
#     print("\u274C Could not determine number of CPU cores. TensorFlow using default threading.")

# # --- Global Parameters Configuration ---
# N_STEPS = 5 
# BATCH_SIZE = 256
# EPOCHS = 15

# # --- LIST OF TICKERS ---

# POSTGRES_HOST = os.getenv("POSTGRES_HOST")
# POSTGRES_PORT = os.getenv("POSTGRES_PORT")
# POSTGRES_DB = os.getenv("POSTGRES_DB")
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# def fetch_tickers_from_db():
#     max_retries = 10
#     delay = 5

#     for attempt in range(max_retries):
#         try:
#             conn = psycopg2.connect(
#                 host=POSTGRES_HOST,
#                 port=POSTGRES_PORT,
#                 database=POSTGRES_DB,
#                 user=POSTGRES_USER,
#                 password=POSTGRES_PASSWORD
#             )
#             cursor = conn.cursor()
#             try:
#                 cursor.execute("SELECT DISTINCT ticker FROM companies_info WHERE is_active = TRUE;")
#             except psycopg2.ProgrammingError:
#                 print("Column 'is_active' not found. Falling back to all distinct tickers.")
#                 cursor.execute("SELECT DISTINCT ticker FROM companies_info;")

#             result = cursor.fetchall()
#             tickers = [row[0] for row in result if row[0]]
#             cursor.close()
#             conn.close()

#             if not tickers:
#                 print("No tickers found in the database.")
#             else:
#                 print(f"Loaded {len(tickers)} tickers from DB.")
#             return tickers
#         except Exception as e:
#             print(f"Database not available, retrying in {delay * (attempt + 1)} seconds... ({e})")
#             time.sleep(delay * (attempt + 1))

#     print("Failed to connect to database after multiple attempts. Exiting.")
#     exit(1)

# ALL_TICKERS = fetch_tickers_from_db()
# if not ALL_TICKERS:
#     print("No tickers available from DB. Exiting.")
#     exit(1)


# # Number of worker processes to use for parallel training
# NUM_WORKERS = min(len(ALL_TICKERS), 2)

# CONTAINER_OUTPUT_BASE = "/app/model_lstm"

# MODEL_SAVE_PATH = os.path.join(CONTAINER_OUTPUT_BASE, "models")
# SCALAR_SAVE_PATH = os.path.join(CONTAINER_OUTPUT_BASE, "scalers")
# MAP_SAVE_PATH = CONTAINER_OUTPUT_BASE 
# TICKER_MAP_FILENAME = os.path.join(MAP_SAVE_PATH, "ticker_map.json")

# os.makedirs(MODEL_SAVE_PATH, exist_ok=True)
# os.makedirs(SCALAR_SAVE_PATH, exist_ok=True)
# # --- VARIABLE: KEY FEATURE TO EMPHASIZE ---

# KEY_FEATURE_TO_EMPHASIZE = "price_mean_1min"

# # --- Database Connection Retry Logic ---
# def connect_to_db_with_retries(max_retries=15, delay=5):
#     db_name = os.getenv("POSTGRES_DB")
#     db_user = os.getenv("POSTGRES_USER")
#     db_password = os.getenv("POSTGRES_PASSWORD")
#     db_host = os.getenv("POSTGRES_HOST")
#     db_port = os.getenv("POSTGRES_PORT")

#     db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

#     for i in range(max_retries):
#         try:
#             # print(f"Attempting to connect to PostgreSQL (Attempt {i+1}/{max_retries})...") # Commented for less verbose parallel logs
#             engine = create_engine(db_url)
#             with engine.connect() as connection:
#                 connection.execute(text('SELECT 1'))
#             # print(f"\u2705 Successfully connected to PostgreSQL!") # Commented for less verbose parallel logs
#             return engine
#         except OperationalError as e:
#             sys.stderr.write(f"PostgreSQL connection failed: {e}. Retrying in {delay}s...\n")
#         except Exception as e:
#             sys.stderr.write(f"Unexpected error during DB connection: {e}. Retrying in {delay}s...\n")
#         time.sleep(delay) # Delay is outside the try-except for all retries
#     sys.stderr.write("Max retries reached. Could not connect to PostgreSQL. Exiting.\n")
#     raise Exception("Failed to connect to database.")


# # --- Function for training a single ticker ---
# def train_model_for_ticker(ticker_info):
#     """
#     Wrapper function for training the model for a single ticker.
#     It is executed in a separate process.
#     """
#     current_ticker, ticker_code, total_tickers = ticker_info
    
#     # Each process must have its own DB connection
#     try:
#         engine = connect_to_db_with_retries()
#     except Exception as e:
#         print(f"\u274C Process for ticker {current_ticker}: Failed to connect to DB. Skipping. Error: {e}")
#         return current_ticker, False # Return ticker name and success status

#     print(f"\n{'='*80}")
#     print(f"TRAINING MODEL FOR TICKER: {current_ticker} (Ticker Code: {ticker_code})")
#     print(f"{'='*80}\n")

#     MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, f"lstm_model_{current_ticker}.h5")
#     SCALER_FILENAME = os.path.join(SCALAR_SAVE_PATH, f"scaler_{current_ticker}.pkl")

#     try:
#         print(f"\U0001F535 [{current_ticker}] Step 1: Loading Data")
#         query = f"SELECT * FROM aggregated_data WHERE ticker = '{current_ticker}' ORDER BY timestamp"
#         df = pd.read_sql(query, engine)
#         print(f"\u2705 [{current_ticker}] Data loaded: {df.shape}")

#         if df.empty:
#             print(f"\u274C [{current_ticker}] No data found. Skipping.")
#             return current_ticker, False

#         print(f"\n\U0001F535 [{current_ticker}] Step 2: Preprocessing and Memory Optimization")
#         initial_rows = df.shape[0]
#         df = df.dropna()
#         print(f"   \u27A1 [{current_ticker}] Removed {initial_rows - df.shape[0]} rows with NaNs. Remaining: {df.shape[0]}")

#         if df.empty:
#             print(f"\u274C [{current_ticker}] No data remaining after NaN removal. Skipping.")
#             return current_ticker, False

#         df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
#         df = df.sort_values('timestamp')

#         for col in df.select_dtypes(include=['float64']).columns:
#             df[col] = df[col].astype(np.float32)
#         for col in df.select_dtypes(include=['int64']).columns:
#             if df[col].min() >= np.iinfo(np.int8).min and df[col].max() <= np.iinfo(np.int8).max:
#                 df[col] = df[col].astype(np.int8)
#             elif df[col].min() >= np.iinfo(np.int16).min and df[col].max() <= np.iinfo(np.int16).max:
#                 df[col] = df[col].astype(np.int16)
#             elif df[col].min() >= np.iinfo(np.int32).min and df[col].max() <= np.iinfo(np.int32).max:
#                 df[col] = df[col].astype(np.int32)
        
#         # 'ticker' and 'ticker_code' might not always be present in the DataFrame
#         # after the query, but it's good practice to remove them if present and not used as features.
#         if 'ticker' in df.columns:
#             df = df.drop(columns=['ticker'])
#         # NOTE: Here we re-insert 'ticker_code' as a separate model input
#         # so it should not be scaled with other features.
#         # Make sure it's not already a feature in your DB if you don't want to scale it.
#         if 'ticker_code' in df.columns:
#             df = df.drop(columns=['ticker_code'])


#         df = df.rename(columns={'y1': 'y'})
#         feature_cols = [c for c in df.columns if c not in ['timestamp', 'y']]
#         print(f"\u2705 [{current_ticker}] Preprocessing complete. Features: {feature_cols}")

#         print(f"\n\U0001F535 [{current_ticker}] Step 2.5: Feature Scaling")
#         scaler = MinMaxScaler()
#         df[feature_cols] = scaler.fit_transform(df[feature_cols])
#         print(f"\u2705 [{current_ticker}] Features scaled (MinMaxScaler).")

#         joblib.dump(scaler, SCALER_FILENAME)
#         print(f"\u2705 [{current_ticker}] Scaler saved to {SCALER_FILENAME}")

#         print(f"\n\U0001F535 [{current_ticker}] Step 3: Optimized Sequence Creation")
#         if len(df) < N_STEPS + 1:
#             print(f"\u274C [{current_ticker}] Insufficient data ({len(df)} points). At least {N_STEPS + 1} needed. Skipping.")
#             return current_ticker, False

#         ticker_features_scaled = df[feature_cols].values.astype(np.float32)
#         ticker_target = df['y'].values.astype(np.float32)

#         # Create sequences
#         # X_seq will be (num_samples, N_STEPS, num_features)
#         sequences = np.lib.stride_tricks.sliding_window_view(ticker_features_scaled, (N_STEPS, ticker_features_scaled.shape[1]))
#         sequences = sequences.squeeze(axis=1) # Remove the extra dimension introduced by sliding_window_view

#         # Targets are the 'y' value following the N_STEPS sequence
#         targets = ticker_target[N_STEPS:]

#         # Ensure dimensions match
#         if len(sequences) > len(targets):
#             sequences = sequences[:len(targets)]
#         elif len(sequences) < len(targets):
#             print(f"\u274C [{current_ticker}] Unexpected error: targets longer than sequences. Targets: {len(targets)}, Sequences: {len(sequences)}. Skipping.")
#             return current_ticker, False

#         X_seq = sequences
#         y = targets

#         X_seq_key_feature_only = None
#         key_feature_index = -1

#         # If a key feature is specified, extract it for a separate input
#         if KEY_FEATURE_TO_EMPHASIZE and KEY_FEATURE_TO_EMPHASIZE in feature_cols:
#             key_feature_index = feature_cols.index(KEY_FEATURE_TO_EMPHASIZE)
#             print(f"\u2705 [{current_ticker}] Emphasizing feature: '{KEY_FEATURE_TO_EMPHASIZE}' at index {key_feature_index}")
#             # Extract the sequence of the key feature (shape: (samples, N_STEPS, 1))
#             X_seq_key_feature_only = X_seq[:, :, key_feature_index:key_feature_index+1]
        
#         print(f"\u2705 [{current_ticker}] Sequences created: X_seq={X_seq.shape}, y={y.shape}")

#         # Train/validation split
#         if X_seq_key_feature_only is not None:
#             X_seq_train, X_seq_val, X_seq_key_feature_only_train, X_seq_key_feature_only_val, y_train, y_val = train_test_split(
#                 X_seq, X_seq_key_feature_only, y, test_size=0.2, random_state=42
#             )
#         else:
#             X_seq_train, X_seq_val, y_train, y_val = train_test_split(
#                 X_seq, y, test_size=0.2, random_state=42
#             )

#         print(f"\u2705 [{current_ticker}] Dataset ready: train={len(X_seq_train)} samples, val={len(X_seq_val)} samples")

#         print(f"\n\U0001F535 [{current_ticker}] Step 4: Building LSTM Model with Ticker Code and (optional) Key Feature Input")
#         num_features = len(feature_cols)

#         # Input for the main feature sequence
#         input_seq = layers.Input(shape=(N_STEPS, num_features), name='input_sequence')
        
#         # Input for the ticker code (a single integer value to identify the ticker)
#         input_ticker_code = layers.Input(shape=(1,), name='input_ticker_code', dtype=tf.int32)
        
#         # Embedding layer for the ticker code
#         max_ticker_code = len(ALL_TICKERS) + 1 # A safe value that includes all possible codes
#         embedding_dim = 4 # Embedding dimension
#         ticker_embedding = layers.Embedding(input_dim=max_ticker_code, output_dim=embedding_dim)(input_ticker_code)
#         ticker_embedding = layers.Flatten()(ticker_embedding) # Flatten the embedding

#         # LSTM for feature sequences
#         lstm_out = layers.LSTM(64, return_sequences=False)(input_seq)
        
#         # List of inputs to concatenate
#         merged_inputs = [lstm_out, ticker_embedding]
#         model_inputs = [input_seq, input_ticker_code]

#         # If the key feature is specified, add its input stream
#         if X_seq_key_feature_only is not None:
#             input_key_feature_sequence = layers.Input(shape=(N_STEPS, 1), name='input_key_feature_sequence')
#             # Apply a dense neural network to each time step of the key feature
#             key_feature_processed = layers.TimeDistributed(layers.Dense(8, activation='relu'))(input_key_feature_sequence)
#             key_feature_processed = layers.Flatten()(key_feature_processed) # Flatten the sequential output
            
#             merged_inputs.append(key_feature_processed)
#             model_inputs.append(input_key_feature_sequence)

#         # Concatenate all processed inputs
#         merged = layers.concatenate(merged_inputs)

#         # Dense layers
#         x = layers.Dense(32, activation='relu')(merged)
#         output = layers.Dense(1)(x)

#         # Define the model with all inputs
#         model = models.Model(inputs=model_inputs, outputs=output)
#         model.compile(optimizer='adam', loss='mse')
#         model.summary()
#         print(f"\u2705 [{current_ticker}] Model built")

#         print(f"\n\U0001F535 [{current_ticker}] Step 5: Training")
#         # For training, we need to provide all inputs to the model
#         # Create ticker code arrays and, if present, key feature arrays
#         ticker_code_array_train = np.full((X_seq_train.shape[0], 1), ticker_code, dtype=np.int32)
#         ticker_code_array_val = np.full((X_seq_val.shape[0], 1), ticker_code, dtype=np.int32)

#         # Prepare training and validation data based on model inputs
#         train_data_inputs = [X_seq_train, ticker_code_array_train]
#         val_data_inputs = [X_seq_val, ticker_code_array_val]
#         if X_seq_key_feature_only is not None:
#             train_data_inputs.append(X_seq_key_feature_only_train)
#             val_data_inputs.append(X_seq_key_feature_only_val)

#         # Recreate TensorFlow Datasets with all inputs
#         dataset_train = tf.data.Dataset.from_tensor_slices((tuple(train_data_inputs), y_train))
#         dataset_train = dataset_train.shuffle(1000).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#         dataset_val = tf.data.Dataset.from_tensor_slices((tuple(val_data_inputs), y_val))
#         dataset_val = dataset_val.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#         history = model.fit(dataset_train, validation_data=dataset_val, epochs=EPOCHS, verbose=2) # verbose=2 for less detailed output
#         print(f"\u2705 [{current_ticker}] Training complete")

#         print(f"\n\U0001F535 [{current_ticker}] Step 6: Model Saving")
#         model.save(MODEL_FILENAME)
#         print(f"\u2705 [{current_ticker}] Model saved to {MODEL_FILENAME}")
#         return current_ticker, True

#     except Exception as e:
#         print(f"\u274C [{current_ticker}] Error during training: {e}")
#         return current_ticker, False


# # --- Main Training Workflow (Parallelized) ---
# if __name__ == "__main__":
#     print("\n" + "="*80)
#     print("STARTING BATCH TRAINING FOR ALL TICKERS (PARALLELIZED)")
#     print("="*80 + "\n")

#     # Map ticker names to numerical codes
#     # This is crucial because the model expects a numerical input for the ticker
#     # and must be consistent between training and inference.
#     ticker_name_to_code_map = {ticker: i for i, ticker in enumerate(ALL_TICKERS)}

#     # Save the ticker map
#     os.makedirs(MAP_SAVE_PATH, exist_ok=True)
#     with open(TICKER_MAP_FILENAME, 'w') as f:
#         json.dump(ticker_name_to_code_map, f)
#     print(f"\u2705 Ticker mapping saved to {TICKER_MAP_FILENAME}")

#     # Prepare the list of arguments for the processes (ticker, ticker_code, total_tickers)
#     tasks = [(ticker, ticker_name_to_code_map[ticker], len(ALL_TICKERS)) for ticker in ALL_TICKERS]

#     successful_tickers = []
#     failed_tickers = []

#     # Use a Process Pool
#     with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
#         # map applies the function to each element in the tasks list and waits for all to finish
#         results = pool.map(train_model_for_ticker, tasks)

#     for ticker, success in results:
#         if success:
#             successful_tickers.append(ticker)
#         else:
#             failed_tickers.append(ticker)

#     print("\n" + "="*80)
#     print("ALL BATCH TRAINING COMPLETED")
#     print("="*80 + "\n")
#     print(f"\u2705 Successfully trained models for: {successful_tickers}")
#     if failed_tickers:
#         print(f"\u274C Failed to train models for: {failed_tickers}")
#         sys.exit(1) # Exit with error if some trainings failed
#     else:
#         print("All models trained successfully!")
    
#     sys.exit(0)





# import os
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from tensorflow.keras import layers, models
# from sklearn.model_selection import train_test_split
# from sqlalchemy import create_engine, text
# import psycopg2
# import time
# from psycopg2 import OperationalError
# from sklearn.preprocessing import MinMaxScaler
# import joblib # Import to save/load the scaler
# import sys
# import multiprocessing # Import for parallelization
# import json # To save the ticker_map
# from kafka import KafkaConsumer # <--- NUOVA IMPORTAZIONE
# import logging # <--- NUOVA IMPORTAZIONE per un logging più pulito
# from collections import defaultdict

# # Configurazione del logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # --- GPU Configuration (if applicable) ---
# num_cpu_cores = os.cpu_count()
# if num_cpu_cores:
#     tf.config.threading.set_inter_op_parallelism_threads(1) # Generally 1 to avoid competition among inter-op threads of a single process
#     tf.config.threading.set_intra_op_parallelism_threads(num_cpu_cores // multiprocessing.cpu_count() if multiprocessing.cpu_count() > 0 else 1) # Divide cores among processes if useful
#     logging.info(f"\u2705 TensorFlow configured for CPU parallelism with {num_cpu_cores} cores detected.")
# else:
#     logging.warning("\u274C Could not determine number of CPU cores. TensorFlow using default threading.")

# # --- Global Parameters Configuration ---
# N_STEPS = 5 
# BATCH_SIZE = 256
# EPOCHS = 15


# # --- KAFKA Configuration for Signal 
# KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# KAFKA_MODEL_START_TOPIC = "start_model"

# # --- LIST OF TICKERS ---

# POSTGRES_HOST = os.getenv("POSTGRES_HOST")
# POSTGRES_PORT = os.getenv("POSTGRES_PORT")
# POSTGRES_DB = os.getenv("POSTGRES_DB")
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# def fetch_tickers_from_db():
#     max_retries = 10
#     delay = 5

#     for attempt in range(max_retries):
#         try:
#             conn = psycopg2.connect(
#                 host=POSTGRES_HOST,
#                 port=POSTGRES_PORT,
#                 database=POSTGRES_DB,
#                 user=POSTGRES_USER,
#                 password=POSTGRES_PASSWORD
#             )
#             cursor = conn.cursor()
#             try:
#                 cursor.execute("SELECT DISTINCT ticker FROM companies_info WHERE is_active = TRUE;")
#             except psycopg2.ProgrammingError:
#                 logging.warning("Column 'is_active' not found. Falling back to all distinct tickers.")
#                 cursor.execute("SELECT DISTINCT ticker FROM companies_info;")

#             result = cursor.fetchall()
#             tickers = [row[0] for row in result if row[0]]
#             cursor.close()
#             conn.close()

#             if not tickers:
#                 logging.warning("No tickers found in the database.")
#             else:
#                 logging.info(f"Loaded {len(tickers)} tickers from DB.")
#             return tickers
#         except Exception as e:
#             logging.error(f"Database not available, retrying in {delay * (attempt + 1)} seconds... ({e})", exc_info=True)
#             time.sleep(delay * (attempt + 1))

#     logging.critical("Failed to connect to database after multiple attempts. Exiting.")
#     sys.exit(1) # Termina il processo se non riesce a connettersi

# ALL_TICKERS = [] # Inizializzato vuoto, verrà popolato dopo il segnale
# # ALL_TICKERS = fetch_tickers_from_db() # Questa riga verrà spostata dopo il segnale
# # if not ALL_TICKERS: # Anche questa parte verrà spostata
# #     print("No tickers available from DB. Exiting.")
# #     exit(1)


# # Number of worker processes to use for parallel training
# # NUM_WORKERS = min(len(ALL_TICKERS), 2) # Sarà definito dopo aver caricato ALL_TICKERS

# CONTAINER_OUTPUT_BASE = "/app/models_lstm"

# MODEL_SAVE_PATH = os.path.join(CONTAINER_OUTPUT_BASE, "models")
# SCALAR_SAVE_PATH = os.path.join(CONTAINER_OUTPUT_BASE, "scalers")
# MAP_SAVE_PATH = CONTAINER_OUTPUT_BASE 
# TICKER_MAP_FILENAME = os.path.join(MAP_SAVE_PATH, "ticker_map_prova.json")

# os.makedirs(MODEL_SAVE_PATH, exist_ok=True)
# os.makedirs(SCALAR_SAVE_PATH, exist_ok=True)
# # --- VARIABLE: KEY FEATURE TO EMPHASIZE ---

# KEY_FEATURE_TO_EMPHASIZE = "price_mean_1min"

# # --- Database Connection Retry Logic ---
# def connect_to_db_with_retries(max_retries=15, delay=5):
#     db_name = os.getenv("POSTGRES_DB")
#     db_user = os.getenv("POSTGRES_USER")
#     db_password = os.getenv("POSTGRES_PASSWORD")
#     db_host = os.getenv("POSTGRES_HOST")
#     db_port = os.getenv("POSTGRES_PORT")

#     db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

#     for i in range(max_retries):
#         try:
#             engine = create_engine(db_url)
#             with engine.connect() as connection:
#                 connection.execute(text('SELECT 1'))
#             return engine
#         except OperationalError as e:
#             logging.error(f"PostgreSQL connection failed: {e}. Retrying in {delay}s...", exc_info=True)
#         except Exception as e:
#             logging.error(f"Unexpected error during DB connection: {e}. Retrying in {delay}s...", exc_info=True)
#         time.sleep(delay) # Delay is outside the try-except for all retries
#     logging.critical("Max retries reached. Could not connect to PostgreSQL. Exiting.")
#     raise Exception("Failed to connect to database.")


# # --- Function for training a single ticker ---
# def train_model_for_ticker(ticker_info):
#     """
#     Wrapper function for training the model for a single ticker.
#     It is executed in a separate process.
#     """
#     current_ticker, ticker_code, total_tickers = ticker_info
    
#     # Each process must have its own DB connection
#     try:
#         engine = connect_to_db_with_retries()
#     except Exception as e:
#         logging.error(f"\u274C Process for ticker {current_ticker}: Failed to connect to DB. Skipping. Error: {e}")
#         return current_ticker, False # Return ticker name and success status

#     logging.info(f"\n{'='*80}")
#     logging.info(f"TRAINING MODEL FOR TICKER: {current_ticker} (Ticker Code: {ticker_code})")
#     logging.info(f"{'='*80}\n")

#     MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, f"model_prova_{current_ticker}.h5")
#     SCALER_FILENAME = os.path.join(SCALAR_SAVE_PATH, f"scaler_prova_{current_ticker}.pkl")

#     try:
#         logging.info(f"\U0001F535 [{current_ticker}] Step 1: Loading Data")
#         query = f"SELECT * FROM aggregated_data WHERE ticker = '{current_ticker}' ORDER BY timestamp"
#         df = pd.read_sql(query, engine)
#         logging.info(f"\u2705 [{current_ticker}] Data loaded: {df.shape}")

#         if df.empty:
#             logging.warning(f"\u274C [{current_ticker}] No data found. Skipping.")
#             return current_ticker, False

#         logging.info(f"\n\U0001F535 [{current_ticker}] Step 2: Preprocessing and Memory Optimization")
#         initial_rows = df.shape[0]
#         df = df.dropna()
#         logging.info(f"    \u27A1 [{current_ticker}] Removed {initial_rows - df.shape[0]} rows with NaNs. Remaining: {df.shape[0]}")

#         if df.empty:
#             logging.warning(f"\u274C [{current_ticker}] No data remaining after NaN removal. Skipping.")
#             return current_ticker, False

#         df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
#         df = df.sort_values('timestamp')

#         for col in df.select_dtypes(include=['float64']).columns:
#             df[col] = df[col].astype(np.float32)
#         for col in df.select_dtypes(include=['int64']).columns:
#             if df[col].min() >= np.iinfo(np.int8).min and df[col].max() <= np.iinfo(np.int8).max:
#                 df[col] = df[col].astype(np.int8)
#             elif df[col].min() >= np.iinfo(np.int16).min and df[col].max() <= np.iinfo(np.int16).max:
#                 df[col] = df[col].astype(np.int16)
#             elif df[col].min() >= np.iinfo(np.int32).min and df[col].max() <= np.iinfo(np.int32).max:
#                 df[col] = df[col].astype(np.int32)
        
#         # 'ticker' and 'ticker_code' might not always be present in the DataFrame
#         # after the query, but it's good practice to remove them if present and not used as features.
#         if 'ticker' in df.columns:
#             df = df.drop(columns=['ticker'])
#         # NOTE: Here we re-insert 'ticker_code' as a separate model input
#         # so it should not be scaled with other features.
#         # Make sure it's not already a feature in your DB if you don't want to scale it.
#         if 'ticker_code' in df.columns:
#             df = df.drop(columns=['ticker_code'])


#         df = df.rename(columns={'y1': 'y'})
#         feature_cols = [c for c in df.columns if c not in ['timestamp', 'y']]
#         logging.info(f"\u2705 [{current_ticker}] Preprocessing complete. Features: {feature_cols}")

#         logging.info(f"\n\U0001F535 [{current_ticker}] Step 2.5: Feature Scaling")
#         scaler = MinMaxScaler()
#         df[feature_cols] = scaler.fit_transform(df[feature_cols])
#         logging.info(f"\u2705 [{current_ticker}] Features scaled (MinMaxScaler).")

#         joblib.dump(scaler, SCALER_FILENAME)
#         logging.info(f"\u2705 [{current_ticker}] Scaler saved to {SCALER_FILENAME}")

#         logging.info(f"\n\U0001F535 [{current_ticker}] Step 3: Optimized Sequence Creation")
#         if len(df) < N_STEPS + 1:
#             logging.warning(f"\u274C [{current_ticker}] Insufficient data ({len(df)} points). At least {N_STEPS + 1} needed. Skipping.")
#             return current_ticker, False

#         ticker_features_scaled = df[feature_cols].values.astype(np.float32)
#         ticker_target = df['y'].values.astype(np.float32)

#         # Create sequences
#         # X_seq will be (num_samples, N_STEPS, num_features)
#         sequences = np.lib.stride_tricks.sliding_window_view(ticker_features_scaled, (N_STEPS, ticker_features_scaled.shape[1]))
#         sequences = sequences.squeeze(axis=1) # Remove the extra dimension introduced by sliding_window_view

#         # Targets are the 'y' value following the N_STEPS sequence
#         targets = ticker_target[N_STEPS:]

#         # Ensure dimensions match
#         if len(sequences) > len(targets):
#             sequences = sequences[:len(targets)]
#         elif len(sequences) < len(targets):
#             logging.error(f"\u274C [{current_ticker}] Unexpected error: targets longer than sequences. Targets: {len(targets)}, Sequences: {len(sequences)}. Skipping.")
#             return current_ticker, False

#         X_seq = sequences
#         y = targets

#         X_seq_key_feature_only = None
#         key_feature_index = -1

#         # If a key feature is specified, extract it for a separate input
#         if KEY_FEATURE_TO_EMPHASIZE and KEY_FEATURE_TO_EMPHASIZE in feature_cols:
#             key_feature_index = feature_cols.index(KEY_FEATURE_TO_EMPHASIZE)
#             logging.info(f"\u2705 [{current_ticker}] Emphasizing feature: '{KEY_FEATURE_TO_EMPHASIZE}' at index {key_feature_index}")
#             # Extract the sequence of the key feature (shape: (samples, N_STEPS, 1))
#             X_seq_key_feature_only = X_seq[:, :, key_feature_index:key_feature_index+1]
        
#         logging.info(f"\u2705 [{current_ticker}] Sequences created: X_seq={X_seq.shape}, y={y.shape}")

#         # Train/validation split
#         if X_seq_key_feature_only is not None:
#             X_seq_train, X_seq_val, X_seq_key_feature_only_train, X_seq_key_feature_only_val, y_train, y_val = train_test_split(
#                 X_seq, X_seq_key_feature_only, y, test_size=0.2, random_state=42
#             )
#         else:
#             X_seq_train, X_seq_val, y_train, y_val = train_test_split(
#                 X_seq, y, test_size=0.2, random_state=42
#             )

#         logging.info(f"\u2705 [{current_ticker}] Dataset ready: train={len(X_seq_train)} samples, val={len(X_seq_val)} samples")

#         logging.info(f"\n\U0001F535 [{current_ticker}] Step 4: Building LSTM Model with Ticker Code and (optional) Key Feature Input")
#         num_features = len(feature_cols)

#         # Input for the main feature sequence
#         input_seq = layers.Input(shape=(N_STEPS, num_features), name='input_sequence')
        
#         # Input for the ticker code (a single integer value to identify the ticker)
#         input_ticker_code = layers.Input(shape=(1,), name='input_ticker_code', dtype=tf.int32)
        
#         # Embedding layer for the ticker code
#         max_ticker_code = len(ALL_TICKERS) + 1 # A safe value that includes all possible codes
#         embedding_dim = 4 # Embedding dimension
#         ticker_embedding = layers.Embedding(input_dim=max_ticker_code, output_dim=embedding_dim)(input_ticker_code)
#         ticker_embedding = layers.Flatten()(ticker_embedding) # Flatten the embedding

#         # LSTM for feature sequences
#         lstm_out = layers.LSTM(64, return_sequences=False)(input_seq)
        
#         # List of inputs to concatenate
#         merged_inputs = [lstm_out, ticker_embedding]
#         model_inputs = [input_seq, input_ticker_code]

#         # If the key feature is specified, add its input stream
#         if X_seq_key_feature_only is not None:
#             input_key_feature_sequence = layers.Input(shape=(N_STEPS, 1), name='input_key_feature_sequence')
#             # Apply a dense neural network to each time step of the key feature
#             key_feature_processed = layers.TimeDistributed(layers.Dense(8, activation='relu'))(input_key_feature_sequence)
#             key_feature_processed = layers.Flatten()(key_feature_processed) # Flatten the sequential output
            
#             merged_inputs.append(key_feature_processed)
#             model_inputs.append(input_key_feature_sequence)

#         # Concatenate all processed inputs
#         merged = layers.concatenate(merged_inputs)

#         # Dense layers
#         x = layers.Dense(32, activation='relu')(merged)
#         output = layers.Dense(1)(x)

#         # Define the model with all inputs
#         model = models.Model(inputs=model_inputs, outputs=output)
#         model.compile(optimizer='adam', loss='mse')
#         model.summary(print_fn=logging.info) # Usa logging.info per la summary del modello
#         logging.info(f"\u2705 [{current_ticker}] Model built")

#         logging.info(f"\n\U0001F535 [{current_ticker}] Step 5: Training")
#         # For training, we need to provide all inputs to the model
#         # Create ticker code arrays and, if present, key feature arrays
#         ticker_code_array_train = np.full((X_seq_train.shape[0], 1), ticker_code, dtype=np.int32)
#         ticker_code_array_val = np.full((X_seq_val.shape[0], 1), ticker_code, dtype=np.int32)

#         # Prepare training and validation data based on model inputs
#         train_data_inputs = [X_seq_train, ticker_code_array_train]
#         val_data_inputs = [X_seq_val, ticker_code_array_val]
#         if X_seq_key_feature_only is not None:
#             train_data_inputs.append(X_seq_key_feature_only_train)
#             val_data_inputs.append(X_seq_key_feature_only_val)

#         # Recreate TensorFlow Datasets with all inputs
#         dataset_train = tf.data.Dataset.from_tensor_slices((tuple(train_data_inputs), y_train))
#         dataset_train = dataset_train.shuffle(1000).batch(BATCH_SIZE, drop_remainder=False).prefetch(tf.data.AUTOTUNE)

#         dataset_val = tf.data.Dataset.from_tensor_slices((tuple(val_data_inputs), y_val))
#         dataset_val = dataset_val.batch(BATCH_SIZE, drop_remainder=False).prefetch(tf.data.AUTOTUNE)

#         history = model.fit(dataset_train, validation_data=dataset_val, epochs=EPOCHS, verbose=2) # verbose=2 for less detailed output
#         logging.info(f"\u2705 [{current_ticker}] Training complete")

#         logging.info(f"\n\U0001F535 [{current_ticker}] Step 6: Model Saving")
#         model.save(MODEL_FILENAME)
#         logging.info(f"\u2705 [{current_ticker}] Model saved to {MODEL_FILENAME}")
#         return current_ticker, True

#     except Exception as e:
#         logging.error(f"\u274C [{current_ticker}] Error during training: {e}", exc_info=True)
#         return current_ticker, False


# # --- Main Training Workflow (Parallelized) ---
# if __name__ == "_main_":
#     logging.info("\n" + "="*80)
#     logging.info("STARTING MODEL TRAINING PROCESS")
#     logging.info("Waiting for signal from Flink on Kafka topic: %s", KAFKA_MODEL_START_TOPIC)
#     logging.info("="*80 + "\n")

#     # Inizializza il KafkaConsumer
#     consumer = None
#     try:
#         consumer = KafkaConsumer(
#             KAFKA_MODEL_START_TOPIC,
#             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#             auto_offset_reset='earliest', # Inizia a leggere dall'inizio del topic se non ci sono offset salvati
#             group_id='model_trainer_group', # Usa un group_id per salvare l'offset
#             enable_auto_commit=True, # Abilita il commit automatico dell'offset
#             value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializza i messaggi JSON
#         )
        
#         # Aspetta un messaggio sul topic
#         message_received = False
#         for message in consumer:
#             logging.info(f"\u2705 Signal received from Kafka on topic {message.topic}: {message.value}")
#             message_received = True
#             break # Esci dal ciclo una volta ricevuto il primo messaggio
        
#         if not message_received:
#             logging.error("No signal received from Kafka. Exiting.")
#             sys.exit(1)

#     except Exception as e:
#         logging.critical(f"\u274C Error initializing or consuming from Kafka: {e}", exc_info=True)
#         sys.exit(1)
#     finally:
#         if consumer:
#             consumer.close() # Chiudi il consumer una volta ricevuto il messaggio o in caso di errore

#     logging.info("\n" + "="*80)
#     logging.info("SIGNAL RECEIVED. PROCEEDING WITH MODEL TRAINING.")
#     logging.info("="*80 + "\n")

#     # Ora che il segnale è stato ricevuto, carica i ticker
#     ALL_TICKERS = fetch_tickers_from_db()
#     if not ALL_TICKERS:
#         logging.critical("No tickers available from DB after signal. Exiting.")
#         sys.exit(1)
    
#     NUM_WORKERS = min(len(ALL_TICKERS), os.cpu_count() or 1) # Assicurati che NUM_WORKERS sia almeno 1
#     logging.info(f"Using {NUM_WORKERS} worker processes for training.")

#     # Map ticker names to numerical codes
#     # This is crucial because the model expects a numerical input for the ticker
#     # and must be consistent between training and inference.
#     ticker_name_to_code_map = {ticker: i for i, ticker in enumerate(ALL_TICKERS)}

#     # Save the ticker map
#     os.makedirs(MAP_SAVE_PATH, exist_ok=True)
#     with open(TICKER_MAP_FILENAME, 'w') as f:
#         json.dump(ticker_name_to_code_map, f)
#     logging.info(f"\u2705 Ticker mapping saved to {TICKER_MAP_FILENAME}")

#     # Prepare the list of arguments for the processes (ticker, ticker_code, total_tickers)
#     tasks = [(ticker, ticker_name_to_code_map[ticker], len(ALL_TICKERS)) for ticker in ALL_TICKERS]

#     successful_tickers = []
#     failed_tickers = []

#     # Use a Process Pool
#     with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
#         # map applies the function to each element in the tasks list and waits for all to finish
#         results = pool.map(train_model_for_ticker, tasks)

#     for ticker, success in results:
#         if success:
#             successful_tickers.append(ticker)
#         else:
#             failed_tickers.append(ticker)

#     logging.info("\n" + "="*80)
#     logging.info("ALL BATCH TRAINING COMPLETED")
#     logging.info("="*80 + "\n")
#     logging.info(f"\u2705 Successfully trained models for: {successful_tickers}")
#     if failed_tickers:
#         logging.error(f"\u274C Failed to train models for: {failed_tickers}")
#         sys.exit(1) # Exit with error if some trainings failed
#     else:
#         logging.info("All models trained successfully!")
    
#     logging.info("Model training process finished successfully. Exiting.")
#     sys.exit(0) # Termina il processo in modo pulito
















import os
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, models
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine, text
import psycopg2
import time
from psycopg2 import OperationalError
from sklearn.preprocessing import MinMaxScaler
import joblib # Import to save/load the scaler
import sys
import multiprocessing # Import for parallelization
import json # To save the ticker_map
from kafka import KafkaConsumer # <--- NUOVA IMPORTAZIONE
import logging # <--- NUOVA IMPORTAZIONE per un logging più pulito
from collections import defaultdict

# Configurazione del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GPU Configuration (if applicable) ---
num_cpu_cores = os.cpu_count()
if num_cpu_cores:
    tf.config.threading.set_inter_op_parallelism_threads(1) # Generally 1 to avoid competition among inter-op threads of a single process
    tf.config.threading.set_intra_op_parallelism_threads(num_cpu_cores // multiprocessing.cpu_count() if multiprocessing.cpu_count() > 0 else 1) # Divide cores among processes if useful
    logging.info(f"\u2705 TensorFlow configured for CPU parallelism with {num_cpu_cores} cores detected.")
else:
    logging.warning("\u274C Could not determine number of CPU cores. TensorFlow using default threading.")

# --- Global Parameters Configuration ---
N_STEPS = 5 
BATCH_SIZE = 256
EPOCHS = 15

# --- KAFKA Configuration for Signal 
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_MODEL_START_TOPIC = "start_model"

# --- LIST OF TICKERS ---

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def fetch_tickers_from_db():
    max_retries = 10
    delay = 5

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT DISTINCT ticker FROM companies_info WHERE is_active = TRUE;")
            except psycopg2.ProgrammingError:
                logging.warning("Column 'is_active' not found. Falling back to all distinct tickers.")
                cursor.execute("SELECT DISTINCT ticker FROM companies_info;")

            result = cursor.fetchall()
            tickers = [row[0] for row in result if row[0]]
            cursor.close()
            conn.close()

            if not tickers:
                logging.warning("No tickers found in the database.")
            else:
                logging.info(f"Loaded {len(tickers)} tickers from DB.")
            return tickers
        except Exception as e:
            logging.error(f"Database not available, retrying in {delay * (attempt + 1)} seconds... ({e})", exc_info=True)
            time.sleep(delay * (attempt + 1))

    logging.critical("Failed to connect to database after multiple attempts. Exiting.")
    sys.exit(1) # Termina il processo se non riesce a connettersi

ALL_TICKERS = [] # Inizializzato vuoto, verrà popolato dopo il segnale

CONTAINER_OUTPUT_BASE = "/app/models_lstm"

MODEL_SAVE_PATH = os.path.join(CONTAINER_OUTPUT_BASE, "models")
SCALAR_SAVE_PATH = os.path.join(CONTAINER_OUTPUT_BASE, "scalers")
MAP_SAVE_PATH = CONTAINER_OUTPUT_BASE 
TICKER_MAP_FILENAME = os.path.join(MAP_SAVE_PATH, "ticker_map.json")

os.makedirs(MODEL_SAVE_PATH, exist_ok=True)
os.makedirs(SCALAR_SAVE_PATH, exist_ok=True)

# --- VARIABLE: KEY FEATURE TO EMPHASIZE ---
KEY_FEATURE_TO_EMPHASIZE = "price_mean_1min"

# --- Database Connection Retry Logic ---
def connect_to_db_with_retries(max_retries=15, delay=5):
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    for i in range(max_retries):
        try:
            engine = create_engine(db_url)
            with engine.connect() as connection:
                connection.execute(text('SELECT 1'))
            return engine
        except OperationalError as e:
            logging.error(f"PostgreSQL connection failed: {e}. Retrying in {delay}s...", exc_info=True)
        except Exception as e:
            logging.error(f"Unexpected error during DB connection: {e}. Retrying in {delay}s...", exc_info=True)
        time.sleep(delay) # Delay is outside the try-except for all retries
    logging.critical("Max retries reached. Could not connect to PostgreSQL. Exiting.")
    raise Exception("Failed to connect to database.")

# --- Function for training a single ticker ---
def train_model_for_ticker(ticker_info):
    """
    Wrapper function for training the model for a single ticker.
    It is executed in a separate process.
    """
    current_ticker, ticker_code, total_tickers = ticker_info
    
    # Each process must have its own DB connection
    try:
        engine = connect_to_db_with_retries()
    except Exception as e:
        logging.error(f"\u274C Process for ticker {current_ticker}: Failed to connect to DB. Skipping. Error: {e}")
        return current_ticker, False # Return ticker name and success status

    logging.info(f"\n{'='*80}")
    logging.info(f"TRAINING MODEL FOR TICKER: {current_ticker} (Ticker Code: {ticker_code})")
    logging.info(f"{'='*80}\n")

    MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, f"model_{current_ticker}.h5")
    SCALER_FILENAME = os.path.join(SCALAR_SAVE_PATH, f"scaler_{current_ticker}.pkl")

    try:
        logging.info(f"\U0001F535 [{current_ticker}] Step 1: Loading Data")
        query = f"SELECT * FROM aggregated_data WHERE ticker = '{current_ticker}' ORDER BY timestamp"
        df = pd.read_sql(query, engine)
        logging.info(f"\u2705 [{current_ticker}] Data loaded: {df.shape}")

        if df.empty:
            logging.warning(f"\u274C [{current_ticker}] No data found. Skipping.")
            return current_ticker, False

        logging.info(f"\n\U0001F535 [{current_ticker}] Step 2: Preprocessing and Memory Optimization")
        initial_rows = df.shape[0]
        df = df.dropna()
        logging.info(f"    \u27A1 [{current_ticker}] Removed {initial_rows - df.shape[0]} rows with NaNs. Remaining: {df.shape[0]}")

        if df.empty:
            logging.warning(f"\u274C [{current_ticker}] No data remaining after NaN removal. Skipping.")
            return current_ticker, False

        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df = df.sort_values('timestamp')

        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].astype(np.float32)
        for col in df.select_dtypes(include=['int64']).columns:
            if df[col].min() >= np.iinfo(np.int8).min and df[col].max() <= np.iinfo(np.int8).max:
                df[col] = df[col].astype(np.int8)
            elif df[col].min() >= np.iinfo(np.int16).min and df[col].max() <= np.iinfo(np.int16).max:
                df[col] = df[col].astype(np.int16)
            elif df[col].min() >= np.iinfo(np.int32).min and df[col].max() <= np.iinfo(np.int32).max:
                df[col] = df[col].astype(np.int32)
        
        if 'ticker' in df.columns:
            df = df.drop(columns=['ticker'])
        if 'ticker_code' in df.columns:
            df = df.drop(columns=['ticker_code'])

        df = df.rename(columns={'y1': 'y'})
        feature_cols = [c for c in df.columns if c not in ['timestamp', 'y']]
        logging.info(f"\u2705 [{current_ticker}] Preprocessing complete. Features: {feature_cols}")

        logging.info(f"\n\U0001F535 [{current_ticker}] Step 2.5: Feature Scaling")
        scaler = MinMaxScaler()
        df[feature_cols] = scaler.fit_transform(df[feature_cols])
        logging.info(f"\u2705 [{current_ticker}] Features scaled (MinMaxScaler).")

        joblib.dump(scaler, SCALER_FILENAME)
        logging.info(f"\u2705 [{current_ticker}] Scaler saved to {SCALER_FILENAME}")

        logging.info(f"\n\U0001F535 [{current_ticker}] Step 3: Optimized Sequence Creation")
        if len(df) < N_STEPS + 1:
            logging.warning(f"\u274C [{current_ticker}] Insufficient data ({len(df)} points). At least {N_STEPS + 1} needed. Skipping.")
            return current_ticker, False

        ticker_features_scaled = df[feature_cols].values.astype(np.float32)
        ticker_target = df['y'].values.astype(np.float32)

        # Create sequences
        sequences = np.lib.stride_tricks.sliding_window_view(ticker_features_scaled, (N_STEPS, ticker_features_scaled.shape[1]))
        sequences = sequences.squeeze(axis=1) # Remove the extra dimension introduced by sliding_window_view

        # Targets are the 'y' value following the N_STEPS sequence
        targets = ticker_target[N_STEPS:]

        # Ensure dimensions match
        if len(sequences) > len(targets):
            sequences = sequences[:len(targets)]
        elif len(sequences) < len(targets):
            logging.error(f"\u274C [{current_ticker}] Unexpected error: targets longer than sequences. Targets: {len(targets)}, Sequences: {len(sequences)}. Skipping.")
            return current_ticker, False

        X_seq = sequences
        y = targets

        X_seq_key_feature_only = None
        key_feature_index = -1

        # If a key feature is specified, extract it for a separate input
        if KEY_FEATURE_TO_EMPHASIZE and KEY_FEATURE_TO_EMPHASIZE in feature_cols:
            key_feature_index = feature_cols.index(KEY_FEATURE_TO_EMPHASIZE)
            logging.info(f"\u2705 [{current_ticker}] Emphasizing feature: '{KEY_FEATURE_TO_EMPHASIZE}' at index {key_feature_index}")
            # Extract the sequence of the key feature (shape: (samples, N_STEPS, 1))
            X_seq_key_feature_only = X_seq[:, :, key_feature_index:key_feature_index+1]
        
        logging.info(f"\u2705 [{current_ticker}] Sequences created: X_seq={X_seq.shape}, y={y.shape}")

        # Train/validation split
        if X_seq_key_feature_only is not None:
            X_seq_train, X_seq_val, X_seq_key_feature_only_train, X_seq_key_feature_only_val, y_train, y_val = train_test_split(
                X_seq, X_seq_key_feature_only, y, test_size=0.2, random_state=42
            )
        else:
            X_seq_train, X_seq_val, y_train, y_val = train_test_split(
                X_seq, y, test_size=0.2, random_state=42
            )

        logging.info(f"\u2705 [{current_ticker}] Dataset ready: train={len(X_seq_train)} samples, val={len(X_seq_val)} samples")

        logging.info(f"\n\U0001F535 [{current_ticker}] Step 4: Building LSTM Model with Ticker Code and (optional) Key Feature Input")
        num_features = len(feature_cols)

        # Input for the main feature sequence
        input_seq = layers.Input(shape=(N_STEPS, num_features), name='input_sequence')
        
        # Input for the ticker code (a single integer value to identify the ticker)
        input_ticker_code = layers.Input(shape=(1,), name='input_ticker_code', dtype=tf.int32)
        
        # Embedding layer for the ticker code
        max_ticker_code = len(ALL_TICKERS) + 1 # A safe value that includes all possible codes
        embedding_dim = 4 # Embedding dimension
        ticker_embedding = layers.Embedding(input_dim=max_ticker_code, output_dim=embedding_dim)(input_ticker_code)
        ticker_embedding = layers.Flatten()(ticker_embedding) # Flatten the embedding

        # LSTM for feature sequences
        lstm_out = layers.LSTM(64, return_sequences=False)(input_seq)
        
        # List of inputs to concatenate
        merged_inputs = [lstm_out, ticker_embedding]
        model_inputs = [input_seq, input_ticker_code]

        # If the key feature is specified, add its input stream
        if X_seq_key_feature_only is not None:
            input_key_feature_sequence = layers.Input(shape=(N_STEPS, 1), name='input_key_feature_sequence')
            # Apply a dense neural network to each time step of the key feature
            key_feature_processed = layers.TimeDistributed(layers.Dense(8, activation='relu'))(input_key_feature_sequence)
            key_feature_processed = layers.Flatten()(key_feature_processed) # Flatten the sequential output
            
            merged_inputs.append(key_feature_processed)
            model_inputs.append(input_key_feature_sequence)

        # Concatenate all processed inputs
        merged = layers.concatenate(merged_inputs)

        # Dense layers
        x = layers.Dense(32, activation='relu')(merged)
        output = layers.Dense(1)(x)

        # Define the model with all inputs
        model = models.Model(inputs=model_inputs, outputs=output)
        model.compile(optimizer='adam', loss='mse')
        model.summary(print_fn=logging.info) # Usa logging.info per la summary del modello
        logging.info(f"\u2705 [{current_ticker}] Model built")

        logging.info(f"\n\U0001F535 [{current_ticker}] Step 5: Training")
        # For training, we need to provide all inputs to the model
        # Create ticker code arrays and, if present, key feature arrays
        ticker_code_array_train = np.full((X_seq_train.shape[0], 1), ticker_code, dtype=np.int32)
        ticker_code_array_val = np.full((X_seq_val.shape[0], 1), ticker_code, dtype=np.int32)

        # Prepare training and validation data based on model inputs
        train_data_inputs = [X_seq_train, ticker_code_array_train]
        val_data_inputs = [X_seq_val, ticker_code_array_val]
        if X_seq_key_feature_only is not None:
            train_data_inputs.append(X_seq_key_feature_only_train)
            val_data_inputs.append(X_seq_key_feature_only_val)

        # Recreate TensorFlow Datasets with all inputs
        dataset_train = tf.data.Dataset.from_tensor_slices((tuple(train_data_inputs), y_train))
        dataset_train = dataset_train.shuffle(1000).batch(BATCH_SIZE, drop_remainder=False).prefetch(tf.data.AUTOTUNE)

        dataset_val = tf.data.Dataset.from_tensor_slices((tuple(val_data_inputs), y_val))
        dataset_val = dataset_val.batch(BATCH_SIZE, drop_remainder=False).prefetch(tf.data.AUTOTUNE)

        history = model.fit(dataset_train, validation_data=dataset_val, epochs=EPOCHS, verbose=2) # verbose=2 for less detailed output
        logging.info(f"\u2705 [{current_ticker}] Training complete")

        logging.info(f"\n\U0001F535 [{current_ticker}] Step 6: Model Saving")
        model.save(MODEL_FILENAME)
        logging.info(f"\u2705 [{current_ticker}] Model saved to {MODEL_FILENAME}")
        return current_ticker, True

    except Exception as e:
        logging.error(f"\u274C [{current_ticker}] Error during training: {e}", exc_info=True)
        return current_ticker, False

# --- Funzione per aspettare il segnale Kafka ---
def wait_for_kafka_signal():
    """Aspetta il segnale da Kafka prima di iniziare il training."""
    logging.info("Waiting for signal from Flink on Kafka topic: %s", KAFKA_MODEL_START_TOPIC)
    
    consumer = None
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_MODEL_START_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id='model_trainer_group',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=30000  # 30 secondi timeout per poll
            )
            
            logging.info(f"Kafka consumer initialized successfully (attempt {attempt + 1})")
            break
            
        except Exception as e:
            logging.error(f"Failed to initialize Kafka consumer (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.critical("Failed to initialize Kafka consumer after all attempts. Exiting.")
                sys.exit(1)
    
    try:
        logging.info("Listening for messages on Kafka topic...")
        
        # Loop infinito per aspettare il messaggio
        while True:
            try:
                # Poll per messaggi con timeout
                message_batch = consumer.poll(timeout_ms=5000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            logging.info(f"\u2705 Signal received from Kafka on topic {message.topic}: {message.value}")
                            return True
                else:
                    # Nessun messaggio ricevuto, continua ad aspettare
                    logging.debug("No messages received, continuing to wait...")
                    
            except Exception as e:
                logging.error(f"Error while polling Kafka: {e}")
                time.sleep(5)  # Aspetta 5 secondi prima di riprovare
                
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
        return False
    except Exception as e:
        logging.error(f"Unexpected error while waiting for Kafka signal: {e}", exc_info=True)
        return False
    finally:
        if consumer:
            consumer.close()
            logging.info("Kafka consumer closed")

# --- Main Training Workflow (Parallelized) ---
if __name__ == "__main__":  # ← CORREZIONE: underscore doppi
    logging.info("\n" + "="*80)
    logging.info("STARTING MODEL TRAINING PROCESS")
    logging.info("="*80 + "\n")

    # Aspetta il segnale da Kafka
    if not wait_for_kafka_signal():
        logging.critical("Failed to receive signal from Kafka. Exiting.")
        sys.exit(1)

    logging.info("\n" + "="*80)
    logging.info("SIGNAL RECEIVED. PROCEEDING WITH MODEL TRAINING.")
    logging.info("="*80 + "\n")

    # Ora che il segnale è stato ricevuto, carica i ticker
    ALL_TICKERS = fetch_tickers_from_db()
    if not ALL_TICKERS:
        logging.critical("No tickers available from DB after signal. Exiting.")
        sys.exit(1)
    
    NUM_WORKERS = min(len(ALL_TICKERS), os.cpu_count() or 1)
    logging.info(f"Using {NUM_WORKERS} worker processes for training.")

    # Map ticker names to numerical codes
    ticker_name_to_code_map = {ticker: i for i, ticker in enumerate(ALL_TICKERS)}

    # Save the ticker map
    os.makedirs(MAP_SAVE_PATH, exist_ok=True)
    with open(TICKER_MAP_FILENAME, 'w') as f:
        json.dump(ticker_name_to_code_map, f)
    logging.info(f"\u2705 Ticker mapping saved to {TICKER_MAP_FILENAME}")

    # Prepare the list of arguments for the processes
    tasks = [(ticker, ticker_name_to_code_map[ticker], len(ALL_TICKERS)) for ticker in ALL_TICKERS]

    successful_tickers = []
    failed_tickers = []

    # Use a Process Pool
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(train_model_for_ticker, tasks)

    for ticker, success in results:
        if success:
            successful_tickers.append(ticker)
        else:
            failed_tickers.append(ticker)

    logging.info("\n" + "="*80)
    logging.info("ALL BATCH TRAINING COMPLETED")
    logging.info("="*80 + "\n")
    logging.info(f"\u2705 Successfully trained models for: {successful_tickers}")
    if failed_tickers:
        logging.error(f"\u274C Failed to train models for: {failed_tickers}")
        sys.exit(1)
    else:
        logging.info("All models trained successfully!")
    
    logging.info("Model training process finished successfully. Exiting.")
    sys.exit(0)