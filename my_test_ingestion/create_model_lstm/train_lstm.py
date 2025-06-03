# import os
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from tensorflow.keras import layers, models
# from sqlalchemy import create_engine, text
# import psycopg2
# import time
# from psycopg2 import OperationalError
# from sklearn.preprocessing import MinMaxScaler
# import joblib # Import for saving/loading scaler
# import json # Import for saving ticker map
# import datetime # For date calculations
# import sys
# # Import for date calculations (relativedelta is more precise for months)
# from dateutil.relativedelta import relativedelta 
# from tensorflow.keras.optimizers import Adam

# # --- Configure TensorFlow CPU threads ---
# num_cpu_cores = os.cpu_count()
# if num_cpu_cores:
#     tf.config.threading.set_inter_op_parallelism_threads(num_cpu_cores)
#     tf.config.threading.set_intra_op_parallelism_threads(num_cpu_cores)
#     print(f"\u2705 TensorFlow configured to use {num_cpu_cores} CPU cores for parallelism.")
# else:
#     print("\u274C Could not determine number of CPU cores. TensorFlow using default threading.")

# # --- Configuration Parameters ---
# N_STEPS = 5 # Number of past time steps to consider for each prediction
# BATCH_SIZE = 128
# EPOCHS = 7 # Increased epochs for better learning
# MONTHS_FOR_VALIDATION = 3 # Number of months from the end of data for validation set
# TICKER_EMBEDDING_DIM = 8 # Increased embedding dimension for ticker

# # --- Paths for saving artifacts ---
# MODEL_SAVE_PATH = "model"
# MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, "lstm_multi_ticker_epochs7.h5")
# SCALER_FILENAME = os.path.join(MODEL_SAVE_PATH, "scaler.pkl")
# TICKER_MAP_FILENAME = os.path.join(MODEL_SAVE_PATH, "ticker_map.json")
# CUSTOM_LEARNING_RATE = 0.01

# # --- Database Connection Retry Logic ---
# def connect_to_db_with_retries(max_retries=15, delay=5):
#     """
#     Attempts to connect to the PostgreSQL database with retries.
#     This ensures the application waits for the DB to be ready.
#     """
#     db_name = os.getenv("DB_NAME", "aggregated-data")
#     db_user = os.getenv("DB_USER", "admin")
#     db_password = os.getenv("DB_PASSWORD", "admin123")
#     db_host = os.getenv("DB_HOST", "postgre") # Use service name 'postgre' for Docker Compose
#     db_port = os.getenv("DB_PORT", "5432")

#     db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

#     for i in range(max_retries):
#         try:
#             print(f"Attempting to connect to PostgreSQL (Attempt {i+1}/{max_retries})...")
#             engine = create_engine(db_url)
#             # Test connection immediately
#             with engine.connect() as connection:
#                 connection.execute(text('SELECT 1')) 
#             print(f"\u2705 Successfully connected to PostgreSQL!")
#             return engine
#         except OperationalError as e:
#             print(f"PostgreSQL connection failed: {e}")
#             if i < max_retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print("Max retries reached. Could not connect to PostgreSQL. Exiting.")
#                 raise 
#         except Exception as e:
#             print(f"An unexpected error occurred during database connection: {e}")
#             if i < max_retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print("Max retries reached due to unexpected error. Exiting.")
#                 raise
#     return None

# # --- Generator Function for Sequences (Modified for dynamic chronological split) ---
# def sequence_generator(db_engine, ticker_splits, n_steps, feature_cols, scaler, ticker_name_to_code_map, is_training):
#     """
#     Generates sequences and corresponding targets for either training or validation,
#     using ticker-specific chronological splits.
#     """
#     for ticker_name_val, split_date in ticker_splits.items():
#         try:
#             query = f"SELECT * FROM aggregated_data WHERE ticker = '{ticker_name_val}'"
            
#             if is_training:
#                 # Data before split_date for training
#                 query += f" AND timestamp < '{split_date}'" 
#             else: # is_validation
#                 # Data from split_date onwards for validation
#                 query += f" AND timestamp >= '{split_date}'" 
            
#             query += " ORDER BY timestamp"
            
#             ticker_df = pd.read_sql(query, db_engine)

#             # Preprocessing for the current ticker_df
#             ticker_df = ticker_df.dropna() # Drop NaNs for this specific ticker
#             ticker_df['timestamp'] = pd.to_datetime(ticker_df['timestamp'], utc=True)
#             ticker_df = ticker_df.sort_values('timestamp') # Ensure sorted by timestamp
#             ticker_df = ticker_df.rename(columns={'y1': 'y'})

#             # Create 'ticker_code' column in memory using the global mapping
#             ticker_df['ticker_code'] = ticker_df['ticker'].map(ticker_name_to_code_map).astype(np.int16)

#             # Apply the *pre-fitted* scaler to the features of this ticker's data
#             if not ticker_df.empty:
#                 ticker_df[feature_cols] = scaler.transform(ticker_df[feature_cols])

#             # Ensure there's enough data for at least one sequence + target
#             if len(ticker_df) >= n_steps + 1: 
#                 ticker_features = ticker_df[feature_cols].values.astype(np.float32)
#                 ticker_target = ticker_df['y'].values.astype(np.float32)
                
#                 # Use sliding_window_view for sequences
#                 # This creates overlapping windows of N_STEPS for features
#                 sequences = np.lib.stride_tricks.sliding_window_view(ticker_features, (n_steps, ticker_features.shape[1]))
#                 # sliding_window_view adds an extra dimension at the beginning, remove it
#                 sequences = sequences.squeeze(axis=1) 

#                 # Target corresponds to the value *after* the sequence.
#                 # If sequence is [t, t+N_STEPS-1], target is y[t+N_STEPS]
#                 targets = ticker_target[n_steps:] 
                
#                 # Ensure sequences and targets align correctly in length
#                 # This handles cases where the last few data points might not form a full sequence
#                 min_len = min(len(sequences), len(targets))
#                 final_sequences = sequences[:min_len]
#                 final_targets = targets[:min_len]

#                 # Create an array of ticker codes, one for each generated sequence-target pair
#                 # All sequences from a single ticker_df will have the same ticker_code
#                 final_ticker_codes = np.full(len(final_targets), ticker_df['ticker_code'].iloc[0], dtype=np.int32)
                
#                 if len(final_sequences) > 0: # Ensure there's data to yield
#                     for i in range(len(final_sequences)):
#                         yield (final_sequences[i], final_ticker_codes[i]), final_targets[i]
#                 else:
#                     print(f"   \u274C Warning: No valid sequences generated for ticker {ticker_name_val} in {'training' if is_training else 'validation'} set.")
#             else:
#                 # Not enough data for this ticker in this date range or for sequence creation
#                 print(f"   \u274C Warning: Not enough data for ticker {ticker_name_val} ({len(ticker_df)} points) to form sequences (N_STEPS={n_steps}) in {'training' if is_training else 'validation'} set.")
#         except Exception as e:
#             print(f"   \u274C Error processing ticker {ticker_name_val} in {'training' if is_training else 'validation'} set: {e}")
#             continue # Continue to the next ticker

# # --- Main Training Workflow ---
# if __name__ == "__main__":
#     print("\U0001F535 Step 1: Connessione e caricamento dati")
#     engine = connect_to_db_with_retries()
#     if engine is None:
#         sys.exit(1)

#     # --- Initial Data Load for Feature Columns, Global Ticker Mapping, and Split Dates ---
#     try:
#         print("   \u27A1 Caricamento di un campione per identificare le colonne delle features...")
#         # Load a larger sample or all data to ensure feature columns are correctly identified
#         # and scaler is fitted on a representative dataset.
#         # For production, consider fitting scaler on a dedicated training subset.
#         sample_df = pd.read_sql("SELECT * FROM aggregated_data", engine) 
        
#         if sample_df.empty:
#             print("\u274C Nessun dato trovato nel database o campione troppo piccolo.")
#             sys.exit(1)

#         sample_df = sample_df.rename(columns={'y1': 'y'})
#         feature_cols = [c for c in sample_df.columns if c not in ['timestamp', 'ticker', 'y']]
#         num_features = len(feature_cols)
        
#         print("   \u27A1 Caricamento di tutti i nomi dei ticker unici e determinazione delle date di split...")
#         distinct_ticker_max_dates_query = "SELECT ticker, MAX(timestamp) as max_timestamp FROM aggregated_data GROUP BY ticker"
#         distinct_ticker_max_dates_df = pd.read_sql(distinct_ticker_max_dates_query, engine)
        
#         if distinct_ticker_max_dates_df.empty:
#             print("\u274C Nessun ticker unico trovato nel database.")
#             sys.exit(1)
        
#         # Create a consistent mapping from ticker name (string) to integer code
#         temp_ticker_series = distinct_ticker_max_dates_df['ticker'].astype('category')
#         ticker_name_to_code_map = {name: code for code, name in enumerate(temp_ticker_series.cat.categories)}
#         num_unique_tickers = len(temp_ticker_series.cat.categories)

#         # Calculate split date for each ticker
#         ticker_split_dates = {}
#         for _, row in distinct_ticker_max_dates_df.iterrows():
#             ticker_name = row['ticker']
#             max_ts = pd.to_datetime(row['max_timestamp'])
#             # Use relativedelta for precise month subtraction
#             split_date = max_ts - relativedelta(months=MONTHS_FOR_VALIDATION) 
#             ticker_split_dates[ticker_name] = split_date.strftime('%Y-%m-%d %H:%M:%S') # Format for SQL query

#         print(f"\u2705 Identificate {num_features} features e {num_unique_tickers} ticker unici (codici numerici).")
#         print(f"\u2705 Calcolate date di split individuali (ultimi {MONTHS_FOR_VALIDATION} mesi) per {num_unique_tickers} ticker.")

#     except Exception as e:
#         print(f"\u274C Errore durante il caricamento del campione, dei ticker o delle date di split: {e}")
#         sys.exit(1)

#     print("\n\U0001F535 Step 2.5: Scaling delle features (su tutti i dati di training)")
#     # Fit the scaler ONLY on the training data to avoid data leakage.
#     # To do this, we'll fetch all training data across all tickers.
#     print("   \u27A1 Caricamento di tutti i dati di training per il fitting dello scaler...")
#     all_training_data = []
#     for ticker_name, split_date_str in ticker_split_dates.items():
#         train_query = f"SELECT * FROM aggregated_data WHERE ticker = '{ticker_name}' AND timestamp < '{split_date_str}' ORDER BY timestamp"
#         ticker_train_df = pd.read_sql(train_query, engine)
#         if not ticker_train_df.empty:
#             all_training_data.append(ticker_train_df[feature_cols])
    
#     if not all_training_data:
#         print("\u274C Nessun dato di training disponibile per il fitting dello scaler. Uscita.")
#         sys.exit(1)

#     combined_training_df = pd.concat(all_training_data)
#     scaler = MinMaxScaler()
#     scaler.fit(combined_training_df[feature_cols]) 
#     print("\u2705 Scaler (MinMaxScaler) addestrato su tutti i dati di training.")
    
#     # --- Save scaler and ticker map ---
#     os.makedirs(MODEL_SAVE_PATH, exist_ok=True)
#     joblib.dump(scaler, SCALER_FILENAME)
#     print(f"\u2705 Scaler salvato in {SCALER_FILENAME}")
#     with open(TICKER_MAP_FILENAME, 'w') as f:
#         json.dump(ticker_name_to_code_map, f)
#     print(f"\u2705 Mappatura ticker salvata in {TICKER_MAP_FILENAME}")
        
#     print("\n\U0001F535 Step 3: Preparazione Dati per Generator (Split Cronologico Dinamico)")

#     # Create TensorFlow Datasets using from_generator
#     # Pass ticker_split_dates to the generator
#     dataset_train = tf.data.Dataset.from_generator(
#         lambda: sequence_generator(engine, ticker_split_dates, N_STEPS, feature_cols, scaler, ticker_name_to_code_map, is_training=True),
#         output_types=( (tf.float32, tf.int32), tf.float32 ), 
#         output_shapes=( (tf.TensorShape([N_STEPS, num_features]), tf.TensorShape([])), tf.TensorShape([]) )
#     )
#     dataset_train = dataset_train.shuffle(1000).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#     dataset_val = tf.data.Dataset.from_generator(
#         lambda: sequence_generator(engine, ticker_split_dates, N_STEPS, feature_cols, scaler, ticker_name_to_code_map, is_training=False),
#         output_types=( (tf.float32, tf.int32), tf.float32 ),
#         output_shapes=( (tf.TensorShape([N_STEPS, num_features]), tf.TensorShape([])), tf.TensorShape([]) )
#     )
#     dataset_val = dataset_val.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#     print(f"\u2705 Dataset generators pronti per il training e la validazione con split cronologico dinamico per ticker.")

#     print("\n\U0001F535 Step 4: Costruzione modello")
#     input_seq = layers.Input(shape=(N_STEPS, num_features), name='input_seq') 
#     input_ticker = layers.Input(shape=(), dtype='int32', name='input_ticker')

#     # Increased output_dim for ticker embedding
#     ticker_embedding = layers.Embedding(input_dim=num_unique_tickers, output_dim=TICKER_EMBEDDING_DIM)(input_ticker)
#     ticker_embedding = layers.RepeatVector(N_STEPS)(ticker_embedding) 

#     x = layers.Concatenate()([input_seq, ticker_embedding])

#     # Potentially add more LSTM layers or increase units for more complexity
#     x = layers.LSTM(128, return_sequences=True)(x) # Increased units, added another LSTM layer
#     x = layers.LSTM(64, return_sequences=False)(x) 
#     x = layers.Dense(64, activation='relu')(x) # Increased units for dense layer
#     x = layers.Dropout(0.2)(x) # Added dropout for regularization
#     output = layers.Dense(1)(x) 

#     model = models.Model(inputs=[input_seq, input_ticker], outputs=output)
#     # Crea un'istanza dell'ottimizzatore Adam con il learning rate personalizzato
#     optimizer = Adam(learning_rate=CUSTOM_LEARNING_RATE) 

#     # Compila il modello passando l'istanza dell'ottimizzatore
#     model.compile(optimizer=optimizer, loss='mse') 
#     model.summary() 
#     print("\u2705 Modello costruito")

#     print("\n\U0001F535 Step 5: Training")
#     history = model.fit(dataset_train, validation_data=dataset_val, epochs=EPOCHS)
#     print("\u2705 Training completato")

#     print("\n\U0001F535 Step 6: Salvataggio modello")
#     # Model filename is defined at the top
#     model.save(MODEL_FILENAME)
#     print(f"\u2705 Modello multi-ticker salvato in {MODEL_FILENAME}")

#     print("\n--- Training Pipeline Completed ---")










# # IN TEORIA QUELLO PIU SENSATO MA HA LOSS MOLTO ALTA E CI METTE MOLTO NON ANCORA PROVATO

import os
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, models
from sqlalchemy import create_engine, text
import psycopg2 # For initial DB connection check
import time # For retry sleep
from psycopg2 import OperationalError # Specific exception for DB connection issues
from sklearn.preprocessing import MinMaxScaler # Or StandardScaler
import sys
from tensorflow.keras.optimizers import Adam

# --- Configure TensorFlow CPU threads ---
num_cpu_cores = os.cpu_count()
if num_cpu_cores:
    tf.config.threading.set_inter_op_parallelism_threads(num_cpu_cores)
    tf.config.threading.set_intra_op_parallelism_threads(num_cpu_cores)
    print(f"\u2705 TensorFlow configured to use {num_cpu_cores} CPU cores for parallelism.")
else:
    print("\u274C Could not determine number of CPU cores. TensorFlow using default threading.")

# --- Configuration Parameters ---
N_STEPS = 5 # Number of past time steps to consider for each prediction
BATCH_SIZE = 128
EPOCHS = 7

# --- Chronological Split Date ---
TRAIN_VAL_SPLIT_DATE = '2024-10-01' # Choose an appropriate date for your data

# --- Database Connection Retry Logic ---
def connect_to_db_with_retries(max_retries=15, delay=5):
    """
    Attempts to connect to the PostgreSQL database with retries.
    This ensures the application waits for the DB to be ready.
    """
    db_name = os.getenv("DB_NAME", "aggregated-data")
    db_user = os.getenv("DB_USER", "admin")
    db_password = os.getenv("DB_PASSWORD", "admin123")
    db_host = os.getenv("DB_HOST", "postgre") # Use service name 'postgre' for Docker Compose
    db_port = os.getenv("DB_PORT", "5432")

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    for i in range(max_retries):
        try:
            print(f"Attempting to connect to PostgreSQL (Attempt {i+1}/{max_retries})...")
            engine = create_engine(db_url)
            # Test connection immediately
            with engine.connect() as connection:
                connection.execute(text('SELECT 1')) 
            print(f"\u2705 Successfully connected to PostgreSQL!")
            return engine
        except OperationalError as e:
            print(f"PostgreSQL connection failed: {e}")
            if i < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Could not connect to PostgreSQL. Exiting.")
                raise 
        except Exception as e:
            print(f"An unexpected error occurred during database connection: {e}")
            if i < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached due to unexpected error. Exiting.")
                raise
    return None

# --- Generator Function for Sequences (Modified for chronological split) ---
def sequence_generator(db_engine, ticker_names_to_process, n_steps, feature_cols, scaler, ticker_name_to_code_map, start_date=None, end_date=None):
    """
    Generates sequences and corresponding targets by loading data for one ticker at a time,
    within a specified date range.
    """
    for ticker_name_val in ticker_names_to_process:
        try:
            # Load data for the current ticker within the specified date range
            query = f"SELECT * FROM aggregated_data WHERE ticker = '{ticker_name_val}'"
            if start_date:
                query += f" AND timestamp >= '{start_date}'"
            if end_date:
                query += f" AND timestamp < '{end_date}'" # Exclude end_date for training, include for validation
            query += " ORDER BY timestamp"
            
            ticker_df = pd.read_sql(query, db_engine)

            # Rest of the generator's code remains the same:
            # Preprocessing for the current ticker_df (similar to main script)
            ticker_df = ticker_df.dropna() # Drop NaNs for this specific ticker
            ticker_df['timestamp'] = pd.to_datetime(ticker_df['timestamp'], utc=True)
            ticker_df = ticker_df.sort_values('timestamp') # Ensure sorted by timestamp
            ticker_df = ticker_df.rename(columns={'y1': 'y'})

            # Create 'ticker_code' column in memory using the global mapping
            ticker_df['ticker_code'] = ticker_df['ticker'].map(ticker_name_to_code_map).astype(np.int16)

            # Apply the *pre-fitted* scaler to the features of this ticker's data
            if not ticker_df.empty:
                ticker_df[feature_cols] = scaler.transform(ticker_df[feature_cols])

            if len(ticker_df) >= n_steps + 1: 
                ticker_features = ticker_df[feature_cols].values.astype(np.float32)
                ticker_target = ticker_df['y'].values.astype(np.float32)
                
                sequences = np.lib.stride_tricks.sliding_window_view(ticker_features, (n_steps, ticker_features.shape[1]))
                sequences = sequences.squeeze(axis=1)

                targets = ticker_target[n_steps:]
                
                final_sequences = sequences[:-1] 
                final_targets = targets
                
                final_ticker_codes = np.full(len(final_targets), ticker_df['ticker_code'].iloc[0], dtype=np.int32)
                
                if len(final_sequences) == len(final_targets) and len(final_sequences) > 0:
                    for i in range(len(final_sequences)):
                        yield (final_sequences[i], final_ticker_codes[i]), final_targets[i]
                else:
                    print(f"  \u274C Warning: Inconsistent lengths for ticker_name {ticker_name_val} in range {start_date}-{end_date} after slicing in generator. Sequences: {len(final_sequences)}, Targets: {len(final_targets)}")
            else:
                pass # Not enough data for this ticker in this date range
        except Exception as e:
            print(f"  \u274C Error processing ticker_name {ticker_name_val} in range {start_date}-{end_date}: {e}")
            continue # Continue to the next ticker

# --- Main Training Workflow ---
if __name__ == "__main__":
    print("\U0001F535 Step 1: Connessione e caricamento dati")
    engine = connect_to_db_with_retries()
    if engine is None:
        sys.exit(1)

    # --- Initial Data Load for Feature Columns and Global Ticker Mapping ---
    try:
        print("  \u27A1 Caricamento di un campione per identificare le colonne delle features...")
        # Load a small sample (without 'ticker_code' in SELECT, as it's not in DB)
        sample_df = pd.read_sql("SELECT * FROM aggregated_data LIMIT 10000", engine) 
        
        if sample_df.empty:
            print("\u274C Nessun dato trovato nel database o campione troppo piccolo.")
            sys.exit(1)

        # Rename target column for consistency
        sample_df = sample_df.rename(columns={'y1': 'y'})

        # Determine feature columns from the sample. Exclude 'timestamp', 'ticker', 'y'.
        feature_cols = [c for c in sample_df.columns if c not in ['timestamp', 'ticker', 'y']]
        num_features = len(feature_cols)
        
        print("  \u27A1 Caricamento di tutti i nomi dei ticker unici per creare una mappatura consistente...")
        # Get all distinct ticker names (strings) from the database
        distinct_ticker_names_df = pd.read_sql("SELECT DISTINCT ticker FROM aggregated_data", engine)
        
        if distinct_ticker_names_df.empty:
            print("\u274C Nessun ticker unico trovato nel database.")
            sys.exit(1)
        
        # Create a consistent mapping from ticker name (string) to integer code
        # We use a temporary Series to leverage .cat.codes for sequential integer assignment
        temp_ticker_series = distinct_ticker_names_df['ticker'].astype('category')
        ticker_name_to_code_map = {name: code for code, name in enumerate(temp_ticker_series.cat.categories)}
        num_unique_tickers = len(temp_ticker_series.cat.categories)

        print(f"\u2705 Identificate {num_features} features e {num_unique_tickers} ticker unici (codici numerici).")

    except Exception as e:
        print(f"\u274C Errore durante il caricamento del campione o dei ticker unici: {e}")
        sys.exit(1)

    print("\n\U0001F535 Step 2.5: Scaling delle features (su un campione)")
    # Initialize and fit the scaler on the numerical features of the sample data.
    # IMPORTANT: In a real-world scenario with chronological splits, the scaler
    # should ideally be fitted ONLY on the training data to avoid data leakage.
    # For this example, we fit it on a general sample.
    scaler = MinMaxScaler()
    scaler.fit(sample_df[feature_cols]) 
    print("\u2705 Scaler (MinMaxScaler) addestrato su un campione di dati.")
        
    print("\n\U0001F535 Step 3: Preparazione Dati per Generator (Split Cronologico)")

    # Now, we use all unique ticker names for both train and validation sets,
    # but the data retrieved by the generator will be split by date.
    all_unique_ticker_names = list(ticker_name_to_code_map.keys())

    print(f"\u2705 Tutti i {len(all_unique_ticker_names)} ticker verranno usati sia per il training che per la validazione con split cronologico.")

    # Create TensorFlow Datasets using from_generator
    # Data for training (all tickers, up to TRAIN_VAL_SPLIT_DATE)
    dataset_train = tf.data.Dataset.from_generator(
        lambda: sequence_generator(engine, all_unique_ticker_names, N_STEPS, feature_cols, scaler, ticker_name_to_code_map, end_date=TRAIN_VAL_SPLIT_DATE),
        output_types=( (tf.float32, tf.int32), tf.float32 ), 
        output_shapes=( (tf.TensorShape([N_STEPS, num_features]), tf.TensorShape([])), tf.TensorShape([]) )
    )
    dataset_train = dataset_train.shuffle(1000).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

    # Data for validation (all tickers, from TRAIN_VAL_SPLIT_DATE onwards)
    dataset_val = tf.data.Dataset.from_generator(
        lambda: sequence_generator(engine, all_unique_ticker_names, N_STEPS, feature_cols, scaler, ticker_name_to_code_map, start_date=TRAIN_VAL_SPLIT_DATE),
        output_types=( (tf.float32, tf.int32), tf.float32 ),
        output_shapes=( (tf.TensorShape([N_STEPS, num_features]), tf.TensorShape([])), tf.TensorShape([]) )
    )
    dataset_val = dataset_val.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

    print(f"\u2705 Dataset generators pronti per il training e la validazione con split cronologico.")

    print("\n\U0001F535 Step 4: Costruzione modello")
    input_seq = layers.Input(shape=(N_STEPS, num_features), name='input_seq') 
    input_ticker = layers.Input(shape=(), dtype='int32', name='input_ticker')

    # Use num_unique_tickers (the count of distinct integer codes) for the embedding layer
    ticker_embedding = layers.Embedding(input_dim=num_unique_tickers, output_dim=8)(input_ticker)
    ticker_embedding = layers.RepeatVector(N_STEPS)(ticker_embedding) 

    x = layers.Concatenate()([input_seq, ticker_embedding])

    x = layers.LSTM(64, return_sequences=False)(x) 
    x = layers.Dense(32, activation='relu')(x) 
    output = layers.Dense(1)(x) 

#     model = models.Model(inputs=[input_seq, input_ticker], outputs=output)
#     # Crea un'istanza dell'ottimizzatore Adam con il learning rate personalizzato
#     optimizer = Adam(learning_rate=CUSTOM_LEARNING_RATE) 

#     # Compila il modello passando l'istanza dell'ottimizzatore
#     model.compile(optimizer=optimizer, loss='mse') 
#     model.summary() 
#     print("\u2705 Modello costruito")
    model = models.Model(inputs=[input_seq, input_ticker], outputs=output)
    #model.compile(optimizer='adam', loss='mse') 
    optimizer = Adam(learning_rate=0.01) 

    # Compila il modello passando l'istanza dell'ottimizzatore
    model.compile(optimizer=optimizer, loss='mse') 
    model.summary() 
    print("\u2705 Modello2 costruito")

    print("\n\U0001F535 Step 5: Training")
    history = model.fit(dataset_train, validation_data=dataset_val, epochs=EPOCHS)
    print("\u2705 Training completato")

    print("\n\U0001F535 Step 6: Salvataggio modello")
    model_save_path = "model"
    os.makedirs(model_save_path, exist_ok=True)
    
    model_filename = os.path.join(model_save_path, "lstm_multi_ticker2.h5")
    model.save(model_filename)
    print(f"\u2705 Modello multi-ticker salvato in {model_filename}")

    print("\n--- Training Pipeline Completed ---")




# import os
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from tensorflow.keras import layers, models
# from sqlalchemy import create_engine, text
# import psycopg2 # For initial DB connection check
# import time # For retry sleep
# from psycopg2 import OperationalError # Specific exception for DB connection issues
# from sklearn.preprocessing import MinMaxScaler # Or StandardScaler
# import sys
# import datetime # For dynamic date calculation
# import json # To save/load best model metrics

# # --- Configure TensorFlow CPU threads ---
# num_cpu_cores = os.cpu_count()
# if num_cpu_cores:
#     tf.config.threading.set_inter_op_parallelism_threads(num_cpu_cores)
#     tf.config.threading.set_intra_op_parallelism_threads(num_cpu_cores)
#     print(f"\u2705 TensorFlow configured to use {num_cpu_cores} CPU cores for parallelism.")
# else:
#     print("\u274C Could not determine number of CPU cores. TensorFlow using default threading.")

# # --- Configuration Parameters ---
# N_STEPS = 30 # Number of past time steps to consider for each prediction
# BATCH_SIZE = 256
# EPOCHS = 5

# # --- Model Saving Paths and Metrics File ---
# MODEL_SAVE_PATH = "model"
# MODEL_FILENAME = os.path.join(MODEL_SAVE_PATH, "lstm_multi_ticker.h5")
# BEST_METRICS_FILE = os.path.join(MODEL_SAVE_PATH, "best_model_metrics.json")

# # --- Database Connection Retry Logic ---
# def connect_to_db_with_retries(max_retries=15, delay=5):
#     """
#     Attempts to connect to the PostgreSQL database with retries.
#     This ensures the application waits for the DB to be ready.
#     """
#     db_name = os.getenv("DB_NAME", "aggregated-data")
#     db_user = os.getenv("DB_USER", "admin")
#     db_password = os.getenv("DB_PASSWORD", "admin123")
#     db_host = os.getenv("DB_HOST", "postgre") # Use service name 'postgre' for Docker Compose
#     db_port = os.getenv("DB_PORT", "5432")

#     db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

#     for i in range(max_retries):
#         try:
#             print(f"Attempting to connect to PostgreSQL (Attempt {i+1}/{max_retries})...")
#             engine = create_engine(db_url)
#             # Test connection immediately
#             with engine.connect() as connection:
#                 connection.execute(text('SELECT 1')) 
#             print(f"\u2705 Successfully connected to PostgreSQL!")
#             return engine
#         except OperationalError as e:
#             print(f"PostgreSQL connection failed: {e}")
#             if i < max_retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print("Max retries reached. Could not connect to PostgreSQL. Exiting.")
#                 raise 
#         except Exception as e:
#             print(f"An unexpected error occurred during database connection: {e}")
#             if i < max_retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print("Max retries reached due to unexpected error. Exiting.")
#                 raise
#     return None

# # --- Generator Function for Sequences (Modified for chronological split) ---
# def sequence_generator(db_engine, ticker_names_to_process, n_steps, feature_cols, scaler, ticker_name_to_code_map, start_date=None, end_date=None):
#     """
#     Generates sequences and corresponding targets by loading data for one ticker at a time,
#     within a specified date range.
#     """
#     for ticker_name_val in ticker_names_to_process:
#         try:
#             # Load data for the current ticker within the specified date range
#             query = f"SELECT * FROM aggregated_data WHERE ticker = '{ticker_name_val}'"
#             if start_date:
#                 query += f" AND timestamp >= '{start_date}'"
#             if end_date:
#                 query += f" AND timestamp < '{end_date}'" # Exclude end_date for training, include for validation
#             query += " ORDER BY timestamp"
            
#             ticker_df = pd.read_sql(query, db_engine)

#             # Preprocessing for the current ticker_df (similar to main script)
#             ticker_df = ticker_df.dropna() # Drop NaNs for this specific ticker
#             ticker_df['timestamp'] = pd.to_datetime(ticker_df['timestamp'], utc=True)
#             ticker_df = ticker_df.sort_values('timestamp') # Ensure sorted by timestamp
#             ticker_df = ticker_df.rename(columns={'y1': 'y'})

#             # Create 'ticker_code' column in memory using the global mapping
#             ticker_df['ticker_code'] = ticker_df['ticker'].map(ticker_name_to_code_map).astype(np.int16)

#             # Apply the *pre-fitted* scaler to the features of this ticker's data
#             if not ticker_df.empty:
#                 ticker_df[feature_cols] = scaler.transform(ticker_df[feature_cols])

#             if len(ticker_df) >= n_steps + 1: 
#                 ticker_features = ticker_df[feature_cols].values.astype(np.float32)
#                 ticker_target = ticker_df['y'].values.astype(np.float32)
                
#                 sequences = np.lib.stride_tricks.sliding_window_view(ticker_features, (n_steps, ticker_features.shape[1]))
#                 sequences = sequences.squeeze(axis=1)

#                 targets = ticker_target[n_steps:]
                
#                 final_sequences = sequences[:-1] 
#                 final_targets = targets
                
#                 final_ticker_codes = np.full(len(final_targets), ticker_df['ticker_code'].iloc[0], dtype=np.int32)
                
#                 if len(final_sequences) == len(final_targets) and len(final_sequences) > 0:
#                     for i in range(len(final_sequences)):
#                         yield (final_sequences[i], final_ticker_codes[i]), final_targets[i]
#                 else:
#                     print(f"  \u274C Warning: Inconsistent lengths for ticker_name {ticker_name_val} in range {start_date}-{end_date} after slicing in generator. Sequences: {len(final_sequences)}, Targets: {len(final_targets)}")
#             else:
#                 pass # Not enough data for this ticker in this date range
#         except Exception as e:
#             print(f"  \u274C Error processing ticker_name {ticker_name_val} in range {start_date}-{end_date}: {e}")
#             continue # Continue to the next ticker

# # --- Main Training Workflow ---
# if __name__ == "__main__":
#     print("\U0001F535 Step 1: Connessione e caricamento dati")
#     engine = connect_to_db_with_retries()
#     if engine is None:
#         sys.exit(1)

#     # --- Initial Data Load for Feature Columns and Global Ticker Mapping ---
#     try:
#         print("  \u27A1 Caricamento di un campione per identificare le colonne delle features...")
#         # Load a small sample (without 'ticker_code' in SELECT, as it's not in DB)
#         # It's better to load the sample *before* the TRAIN_VAL_SPLIT_DATE for scaler fitting
#         query_sample = "SELECT * FROM aggregated_data ORDER BY timestamp DESC LIMIT 10000"
#         sample_df = pd.read_sql(query_sample, engine) 
        
#         if sample_df.empty:
#             print("\u274C Nessun dato trovato nel database o campione troppo piccolo.")
#             sys.exit(1)

#         # Rename target column for consistency
#         sample_df = sample_df.rename(columns={'y1': 'y'})

#         # Determine feature columns from the sample. Exclude 'timestamp', 'ticker', 'y'.
#         feature_cols = [c for c in sample_df.columns if c not in ['timestamp', 'ticker', 'y']]
#         num_features = len(feature_cols)
        
#         print("  \u27A1 Caricamento di tutti i nomi dei ticker unici per creare una mappatura consistente...")
#         # Get all distinct ticker names (strings) from the database
#         distinct_ticker_names_df = pd.read_sql("SELECT DISTINCT ticker FROM aggregated_data", engine)
        
#         if distinct_ticker_names_df.empty:
#             print("\u274C Nessun ticker unico trovato nel database.")
#             sys.exit(1)
        
#         # Create a consistent mapping from ticker name (string) to integer code
#         temp_ticker_series = distinct_ticker_names_df['ticker'].astype('category')
#         ticker_name_to_code_map = {name: code for code, name in enumerate(temp_ticker_series.cat.categories)}
#         num_unique_tickers = len(temp_ticker_series.cat.categories)

#         print(f"\u2705 Identificate {num_features} features e {num_unique_tickers} ticker unici (codici numerici).")

#     except Exception as e:
#         print(f"\u274C Errore durante il caricamento del campione o dei ticker unici: {e}")
#         sys.exit(1)

#     print("\n\U0001F535 Step 2.5: Scaling delle features (su un campione di training)")
#     # IMPORTANT: Fetch data for scaler fitting ONLY from the training period to prevent data leakage.
#     # First, determine the dynamic TRAIN_VAL_SPLIT_DATE.
#     print("  \u27A1 Calcolo dinamico della data di split...")
#     try:
#         max_timestamp_query = "SELECT MAX(timestamp) FROM aggregated_data"
#         max_timestamp_df = pd.read_sql(max_timestamp_query, engine)
#         max_date = pd.to_datetime(max_timestamp_df.iloc[0, 0], utc=True)
        
#         # Define validation period: last 3 months
#         VALIDATION_PERIOD_MONTHS = 3 
        
#         # Calculate split date
#         TRAIN_VAL_SPLIT_DATE = (max_date - pd.DateOffset(months=VALIDATION_PERIOD_MONTHS)).strftime('%Y-%m-%d')
        
#         print(f"\u2705 Data massima nel DB: {max_date.strftime('%Y-%m-%d')}")
#         print(f"\u2705 Periodo di validazione: ultimi {VALIDATION_PERIOD_MONTHS} mesi. Data di split (Training < Validation): {TRAIN_VAL_SPLIT_DATE}")

#         # Fetch training data for scaler fitting (data before TRAIN_VAL_SPLIT_DATE)
#         scaler_fit_query = f"SELECT * FROM aggregated_data WHERE timestamp < '{TRAIN_VAL_SPLIT_DATE}' LIMIT 100000" # Limit to a reasonable size for fitting
#         scaler_fit_df = pd.read_sql(scaler_fit_query, engine)
#         scaler_fit_df = scaler_fit_df.dropna().rename(columns={'y1': 'y'})

#         if scaler_fit_df.empty:
#             print("\u274C Nessun dato di training sufficiente per addestrare lo scaler. Controlla TRAIN_VAL_SPLIT_DATE o i dati.")
#             sys.exit(1)

#         scaler = MinMaxScaler()
#         scaler.fit(scaler_fit_df[feature_cols]) 
#         print("\u2705 Scaler (MinMaxScaler) addestrato su un campione di dati di training.")
            
#     except Exception as e:
#         print(f"\u274C Errore durante il calcolo della data di split o l'addestramento dello scaler: {e}")
#         sys.exit(1)

#     print("\n\U0001F535 Step 3: Preparazione Dati per Generator (Split Cronologico)")

#     # We use all unique ticker names for both train and validation sets.
#     all_unique_ticker_names = list(ticker_name_to_code_map.keys())

#     print(f"\u2705 Tutti i {len(all_unique_ticker_names)} ticker verranno usati sia per il training che per la validazione con split cronologico.")

#     # Create TensorFlow Datasets using from_generator
#     # Data for training (all tickers, up to TRAIN_VAL_SPLIT_DATE)
#     dataset_train = tf.data.Dataset.from_generator(
#         lambda: sequence_generator(engine, all_unique_ticker_names, N_STEPS, feature_cols, scaler, ticker_name_to_code_map, end_date=TRAIN_VAL_SPLIT_DATE),
#         output_types=( (tf.float32, tf.int32), tf.float32 ), 
#         output_shapes=( (tf.TensorShape([N_STEPS, num_features]), tf.TensorShape([])), tf.TensorShape([]) )
#     )
#     dataset_train = dataset_train.shuffle(1000).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#     # Data for validation (all tickers, from TRAIN_VAL_SPLIT_DATE onwards)
#     dataset_val = tf.data.Dataset.from_generator(
#         lambda: sequence_generator(engine, all_unique_ticker_names, N_STEPS, feature_cols, scaler, ticker_name_to_code_map, start_date=TRAIN_VAL_SPLIT_DATE),
#         output_types=( (tf.float32, tf.int32), tf.float32 ),
#         output_shapes=( (tf.TensorShape([N_STEPS, num_features]), tf.TensorShape([])), tf.TensorShape([]) )
#     )
#     dataset_val = dataset_val.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#     print(f"\u2705 Dataset generators pronti per il training e la validazione con split cronologico.")

#     print("\n\U0001F535 Step 4: Costruzione modello")
#     input_seq = layers.Input(shape=(N_STEPS, num_features), name='input_seq') 
#     input_ticker = layers.Input(shape=(), dtype='int32', name='input_ticker')

#     ticker_embedding = layers.Embedding(input_dim=num_unique_tickers, output_dim=4)(input_ticker)
#     ticker_embedding = layers.RepeatVector(N_STEPS)(ticker_embedding) 

#     x = layers.Concatenate()([input_seq, ticker_embedding])

#     x = layers.LSTM(64, return_sequences=False)(x) 
#     x = layers.Dense(32, activation='relu')(x) 
#     output = layers.Dense(1)(x) 

#     model = models.Model(inputs=[input_seq, input_ticker], outputs=output)
#     model.compile(optimizer='adam', loss='mse') 
#     model.summary() 
#     print("\u2705 Modello costruito")

#     print("\n\U0001F535 Step 5: Training")
#     history = model.fit(dataset_train, validation_data=dataset_val, epochs=EPOCHS)
#     print("\u2705 Training completato")

#     print("\n\U0001F535 Step 6: Valutazione e Salvataggio Condizionale del Modello")
    
#     # Evaluate the newly trained model on the validation set
#     val_loss = model.evaluate(dataset_val, verbose=0)
#     print(f"\u2705 Loss di validazione del nuovo modello: {val_loss:.4f}")

#     # Load best historical metrics
#     best_metrics = {'val_loss': float('inf')} # Initialize with a very high loss
#     if os.path.exists(BEST_METRICS_FILE):
#         try:
#             with open(BEST_METRICS_FILE, 'r') as f:
#                 best_metrics = json.load(f)
#             print(f"  \u2139 Caricate metriche del modello precedente. Miglior loss storico: {best_metrics['val_loss']:.4f}")
#         except json.JSONDecodeError:
#             print(f"  \u274C Errore nella lettura del file {BEST_METRICS_FILE}. Lo ignoro.")
            
#     os.makedirs(MODEL_SAVE_PATH, exist_ok=True)

#     # Conditional save logic
#     if val_loss < best_metrics['val_loss']:
#         print(f"\u2705 Nuova loss di validazione ({val_loss:.4f}) è migliore della precedente ({best_metrics['val_loss']:.4f}). Salvataggio del nuovo modello.")
#         model.save(MODEL_FILENAME)
        
#         # Update best metrics
#         best_metrics['val_loss'] = val_loss
#         best_metrics['last_trained_date'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
#         with open(BEST_METRICS_FILE, 'w') as f:
#             json.dump(best_metrics, f)
#         print(f"\u2705 Modello multi-ticker salvato in {MODEL_FILENAME}")
#         print(f"\u2705 Metriche aggiornate salvate in {BEST_METRICS_FILE}")
#     else:
#         print(f"\u274C Nuova loss di validazione ({val_loss:.4f}) non è migliore della precedente ({best_metrics['val_loss']:.4f}). Il modello non è stato salvato.")

#     print("\n--- Training Pipeline Completed ---")




# FUNZIONA IN 6 ORE SU TUTTO MA DIVIDE 26 TICKER PER TRAINING E GLI ALTRI VALIDATION

# import os
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from tensorflow.keras import layers, models
# from sklearn.model_selection import train_test_split
# from sqlalchemy import create_engine, text
# import psycopg2 # For initial DB connection check
# import time # For retry sleep
# from psycopg2 import OperationalError # Specific exception for DB connection issues
# from sklearn.preprocessing import MinMaxScaler # Or StandardScaler
# import sys
# import joblib
# import json

# # --- Configure TensorFlow CPU threads ---
# num_cpu_cores = os.cpu_count()
# if num_cpu_cores:
#     tf.config.threading.set_inter_op_parallelism_threads(num_cpu_cores)
#     tf.config.threading.set_intra_op_parallelism_threads(num_cpu_cores)
#     print(f"\u2705 TensorFlow configured to use {num_cpu_cores} CPU cores for parallelism.")
# else:
#     print("\u274C Could not determine number of CPU cores. TensorFlow using default threading.")

# # --- Configuration Parameters ---
# N_STEPS = 5 # Number of past time steps to consider for each prediction
# BATCH_SIZE = 256
# EPOCHS = 5

# # --- Database Connection Retry Logic ---
# def connect_to_db_with_retries(max_retries=15, delay=5):
#     """
#     Attempts to connect to the PostgreSQL database with retries.
#     This ensures the application waits for the DB to be ready.
#     """
#     db_name = os.getenv("DB_NAME", "aggregated-data")
#     db_user = os.getenv("DB_USER", "admin")
#     db_password = os.getenv("DB_PASSWORD", "admin123")
#     db_host = os.getenv("DB_HOST", "postgre") # Use service name 'postgre' for Docker Compose
#     db_port = os.getenv("DB_PORT", "5432")

#     db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

#     for i in range(max_retries):
#         try:
#             print(f"Attempting to connect to PostgreSQL (Attempt {i+1}/{max_retries})...")
#             engine = create_engine(db_url)
#             # Test connection immediately
#             with engine.connect() as connection:
#                 connection.execute(text('SELECT 1')) 
#             print(f"\u2705 Successfully connected to PostgreSQL!")
#             return engine
#         except OperationalError as e:
#             print(f"PostgreSQL connection failed: {e}")
#             if i < max_retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print("Max retries reached. Could not connect to PostgreSQL. Exiting.")
#                 raise 
#         except Exception as e:
#             print(f"An unexpected error occurred during database connection: {e}")
#             if i < max_retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print("Max retries reached due to unexpected error. Exiting.")
#                 raise
#     return None

# # --- Generator Function for Sequences (Modified for chunked DB loading) ---
# def sequence_generator(db_engine, ticker_names_to_process, n_steps, feature_cols, scaler, ticker_name_to_code_map):
#     """
#     Generates sequences and corresponding targets by loading data for one ticker at a time.
#     This avoids loading the entire dataset into memory.
#     """
#     for ticker_name_val in ticker_names_to_process:
#         try:
#             # Load data for the current ticker directly from the database
#             # We select * and filter by ticker (string name)
#             query = f"SELECT * FROM aggregated_data WHERE ticker = '{ticker_name_val}' ORDER BY timestamp"
#             ticker_df = pd.read_sql(query, db_engine)

#             # Preprocessing for the current ticker_df (similar to main script)
#             ticker_df = ticker_df.dropna() # Drop NaNs for this specific ticker
#             ticker_df['timestamp'] = pd.to_datetime(ticker_df['timestamp'], utc=True)
#             ticker_df = ticker_df.sort_values('timestamp') # Ensure sorted by timestamp
#             ticker_df = ticker_df.rename(columns={'y1': 'y'})

#             # Create 'ticker_code' column in memory using the global mapping
#             ticker_df['ticker_code'] = ticker_df['ticker'].map(ticker_name_to_code_map).astype(np.int16)

#             # Apply the *pre-fitted* scaler to the features of this ticker's data
#             if not ticker_df.empty:
#                 ticker_df[feature_cols] = scaler.transform(ticker_df[feature_cols])

#             if len(ticker_df) >= n_steps + 1: 
#                 ticker_features = ticker_df[feature_cols].values.astype(np.float32)
#                 ticker_target = ticker_df['y'].values.astype(np.float32)
                
#                 sequences = np.lib.stride_tricks.sliding_window_view(ticker_features, (n_steps, ticker_features.shape[1]))
#                 sequences = sequences.squeeze(axis=1)

#                 targets = ticker_target[n_steps:]
                
#                 final_sequences = sequences[:-1] 
#                 final_targets = targets
                
#                 # The ticker code for each sequence will be the same, derived from the loaded data
#                 # We use the ticker_code from the dataframe, which should be consistent
#                 # Use the mapped ticker_code for all sequences from this ticker
#                 final_ticker_codes = np.full(len(final_targets), ticker_df['ticker_code'].iloc[0], dtype=np.int32)
                
#                 if len(final_sequences) == len(final_targets) and len(final_sequences) > 0:
#                     for i in range(len(final_sequences)):
#                         yield (final_sequences[i], final_ticker_codes[i]), final_targets[i]
#                 else:
#                     print(f"  \u274C Warning: Inconsistent lengths for ticker_name {ticker_name_val} after slicing in generator. Sequences: {len(final_sequences)}, Targets: {len(final_targets)}")
#             else:
#                 pass # Not enough data for this ticker
#         except Exception as e:
#             print(f"  \u274C Error processing ticker_name {ticker_name_val}: {e}")
#             continue # Continue to the next ticker

# # --- Main Training Workflow ---
# if __name__ == "__main__":
#     print("\U0001F535 Step 1: Connessione e caricamento dati")
#     engine = connect_to_db_with_retries()
#     if engine is None:
#         sys.exit(1)

#     # --- Initial Data Load for Feature Columns and Global Ticker Mapping ---
#     try:
#         print("  \u27A1 Caricamento di un campione per identificare le colonne delle features...")
#         # Load a small sample (without 'ticker_code' in SELECT, as it's not in DB)
#         sample_df = pd.read_sql("SELECT * FROM aggregated_data LIMIT 10000", engine) 
        
#         if sample_df.empty:
#             print("\u274C Nessun dato trovato nel database o campione troppo piccolo.")
#             sys.exit(1)

#         # Rename target column for consistency
#         sample_df = sample_df.rename(columns={'y1': 'y'})

#         # Determine feature columns from the sample. Exclude 'timestamp', 'ticker', 'y'.
#         feature_cols = [c for c in sample_df.columns if c not in ['timestamp', 'ticker', 'y']]
#         num_features = len(feature_cols)
        
#         print("  \u27A1 Caricamento di tutti i nomi dei ticker unici per creare una mappatura consistente...")
#         # Get all distinct ticker names (strings) from the database
#         distinct_ticker_names_df = pd.read_sql("SELECT DISTINCT ticker FROM aggregated_data", engine)
        
#         if distinct_ticker_names_df.empty:
#             print("\u274C Nessun ticker unico trovato nel database.")
#             sys.exit(1)
        
#         # Create a consistent mapping from ticker name (string) to integer code
#         # We use a temporary Series to leverage .cat.codes for sequential integer assignment
#         temp_ticker_series = distinct_ticker_names_df['ticker'].astype('category')
#         ticker_name_to_code_map = {name: code for code, name in enumerate(temp_ticker_series.cat.categories)}
#         num_unique_tickers = len(temp_ticker_series.cat.categories)

#         print(f"\u2705 Identificate {num_features} features e {num_unique_tickers} ticker unici (codici numerici).")

#     except Exception as e:
#         print(f"\u274C Errore durante il caricamento del campione o dei ticker unici: {e}")
#         sys.exit(1)

#     print("\n\U0001F535 Step 2.5: Scaling delle features (su un campione)")
#     # Initialize and fit the scaler on the numerical features of the sample data.
#     scaler = MinMaxScaler()
#     scaler.fit(sample_df[feature_cols]) 
#     print("\u2705 Scaler (MinMaxScaler) addestrato su un campione di dati.")
        
#     print("\n\U0001F535 Step 3: Preparazione Dati per Generator")

#     # Get the list of all unique ticker names (strings) for splitting
#     all_unique_ticker_names = list(ticker_name_to_code_map.keys())

#     # Split unique ticker names into training and validation sets
#     train_ticker_names, val_ticker_names = train_test_split(
#         all_unique_ticker_names, test_size=0.2, random_state=42, shuffle=True
#     )

#     print(f"\u2705 Ticker divisi: train_ticker_names={len(train_ticker_names)}, val_ticker_names={len(val_ticker_names)}")

#     # Create TensorFlow Datasets using from_generator
#     # Now, the generator takes the database engine, the list of ticker names, and the global map
#     dataset_train = tf.data.Dataset.from_generator(
#         lambda: sequence_generator(engine, train_ticker_names, N_STEPS, feature_cols, scaler, ticker_name_to_code_map),
#         output_types=( (tf.float32, tf.int32), tf.float32 ), 
#         output_shapes=( (tf.TensorShape([N_STEPS, num_features]), tf.TensorShape([])), tf.TensorShape([]) )
#     )
#     dataset_train = dataset_train.shuffle(1000).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#     dataset_val = tf.data.Dataset.from_generator(
#         lambda: sequence_generator(engine, val_ticker_names, N_STEPS, feature_cols, scaler, ticker_name_to_code_map),
#         output_types=( (tf.float32, tf.int32), tf.float32 ),
#         output_shapes=( (tf.TensorShape([N_STEPS, num_features]), tf.TensorShape([])), tf.TensorShape([]) )
#     )
#     dataset_val = dataset_val.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#     print(f"\u2705 Dataset generators pronti per il training e la validazione.")

#     print("\n\U0001F535 Step 4: Costruzione modello.")
#     input_seq = layers.Input(shape=(N_STEPS, num_features), name='input_seq') 
#     input_ticker = layers.Input(shape=(), dtype='int32', name='input_ticker')

#     # Use num_unique_tickers (the count of distinct integer codes) for the embedding layer
#     ticker_embedding = layers.Embedding(input_dim=num_unique_tickers, output_dim=4)(input_ticker)
#     ticker_embedding = layers.RepeatVector(N_STEPS)(ticker_embedding) 

#     x = layers.Concatenate()([input_seq, ticker_embedding])

#     x = layers.LSTM(64, return_sequences=False)(x) 
#     x = layers.Dense(32, activation='relu')(x) 
#     output = layers.Dense(1)(x) 

#     model = models.Model(inputs=[input_seq, input_ticker], outputs=output)
#     model.compile(optimizer='adam', loss='mse') 
#     model.summary() 
#     print("\u2705 Modello costruito")

#     print("\n\U0001F535 Step 5: Training")
#     history = model.fit(dataset_train, validation_data=dataset_val, epochs=EPOCHS)
#     print("\u2705 Training completato")

#     print("\n\U0001F535 Step 6: Salvataggio modello")
#     model_save_path = "model"
#     os.makedirs(model_save_path, exist_ok=True)
    
#     model_filename = os.path.join(model_save_path, "lstm_multi_ticker_step5.h5")
#     model.save(model_filename)
#     print(f"\u2705 Modello multi-ticker salvato in {model_filename}")

#     # --- INSERISCI IL NUOVO CODICE QUI SOTTO ---
#     print("\n\U0001F535 Step 7: Salvataggio Scaler e Mappatura Ticker")
#     SCALER_FILENAME = os.path.join(model_save_path, "scaler.pkl")
#     TICKER_MAP_FILENAME = os.path.join(model_save_path, "ticker_map.json")

#     try:
#         joblib.dump(scaler, SCALER_FILENAME)
#         print(f"\u2705 Scaler salvato in {SCALER_FILENAME}")
        
#         # La mappatura ticker è stata creata: ticker_name_to_code_map
#         # Assicurati che 'ticker_name_to_code_map' sia accessibile qui.
#         # Nel tuo codice, viene creato dopo il preprocessing del DataFrame 'df'.
#         # Per sicurezza, possiamo ricrearlo qui se preferisci, o assicurarci che sia una variabile globale.
#         # Dato che scaler.fit() è chiamato sul df completo, possiamo estrarre la mappa da lì:
#         # Se 'ticker_name_to_code_map' non è globale, ricrealo da df['ticker'].cat.categories
        
#         with open(TICKER_MAP_FILENAME, 'w') as f:
#             json.dump(ticker_name_to_code_map, f)
#         print(f"\u2705 Mappatura ticker salvata in {TICKER_MAP_FILENAME}")

#     except Exception as e:
#         print(f"\u274C Errore durante il salvataggio di scaler o mappatura ticker: {e}")
#     # --- FINE DEL CODICE DA INSERIRE ---



#     print("\n--- Training Pipeline Completed ---")























# FUNZIONA IN 6 MIN PER UN TICKER

# import os
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from tensorflow.keras import layers, models
# from sklearn.model_selection import train_test_split
# from sqlalchemy import create_engine, text
# import psycopg2 # For initial DB connection check
# import time # For retry sleep
# from psycopg2 import OperationalError # Specific exception for DB connection issues
# from sklearn.preprocessing import MinMaxScaler # Or StandardScaler
# import sys

# # --- Configure TensorFlow CPU threads ---
# # Get the number of CPU cores available to the Docker container
# # os.cpu_count() will give the total number of cores available to the *Python process*,
# # which should reflect the Docker container's allocated cores.
# num_cpu_cores = os.cpu_count()
# if num_cpu_cores:
#     tf.config.threading.set_inter_op_parallelism_threads(num_cpu_cores)
#     tf.config.threading.set_intra_op_parallelism_threads(num_cpu_cores)
#     print(f"\u2705 TensorFlow configured to use {num_cpu_cores} CPU cores for parallelism.")
# else:
#     print("\u274C Could not determine number of CPU cores. TensorFlow using default threading.")
# # By setting set_inter_op_parallelism_threads and set_intra_op_parallelism_threads to 
# # os.cpu_count(), you ensure TensorFlow is configured to utilize all available CPU cores 
# # within the Docker container for both parallelizing independent operations and breaking 
# # down single large operations.

# # --- Configuration Parameters ---
# N_STEPS = 30 # Number of past time steps to consider for each prediction
# BATCH_SIZE = 256
# EPOCHS = 5

# # --- Database Connection Retry Logic ---
# def connect_to_db_with_retries(max_retries=15, delay=5):
#     """
#     Attempts to connect to the PostgreSQL database with retries.
#     This ensures the application waits for the DB to be ready.
#     """
#     db_name = os.getenv("DB_NAME", "aggregated-data")
#     db_user = os.getenv("DB_USER", "admin")
#     db_password = os.getenv("DB_PASSWORD", "admin123")
#     db_host = os.getenv("DB_HOST", "postgre") # Use service name 'postgre' for Docker Compose
#     db_port = os.getenv("DB_PORT", "5432")

#     db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

#     for i in range(max_retries):
#         try:
#             print(f"Attempting to connect to PostgreSQL (Attempt {i+1}/{max_retries})...")
#             engine = create_engine(db_url)
#             # Test connection immediately
#             with engine.connect() as connection:
#                 connection.execute(text('SELECT 1')) # Use tf.text.SQL for SQLAlchemy 2.0 or just text() for older
#             print(f"\u2705 Successfully connected to PostgreSQL!")
#             return engine
#         except OperationalError as e:
#             print(f"PostgreSQL connection failed: {e}")
#             if i < max_retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print("Max retries reached. Could not connect to PostgreSQL. Exiting.")
#                 raise # Re-raise the exception if max retries reached
#         except Exception as e:
#             print(f"An unexpected error occurred during database connection: {e}")
#             if i < max_retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print("Max retries reached due to unexpected error. Exiting.")
#                 raise
#     return None

# # --- Main Training Workflow ---
# if __name__ == "__main__":
#     print("\U0001F535 Step 1: Connessione e caricamento dati")
#     # Connect to database with retry logic
#     engine = connect_to_db_with_retries()
#     if engine is None:
#         exit(1) # Exit if no connection could be established

#     # Load all aggregated data
#     df = pd.read_sql("SELECT * FROM aggregated_data", engine)
#     print(f"\u2705 Dati caricati: {df.shape}")

#     print("\n\U0001F535 Step 2: Preprocessing e Ottimizzazione Memoria")
#     # Drop rows with any NaN values. Consider imputation strategies for production.
#     initial_rows = df.shape[0]
#     df = df.dropna()
#     print(f"  \u27A1 Dropped {initial_rows - df.shape[0]} rows with NaNs. Remaining: {df.shape[0]}")

#     df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True) # Ensure timezone awareness
#     df = df.sort_values(['ticker', 'timestamp'])

#     # Optimize data types to reduce memory footprint
#     initial_memory = df.memory_usage(deep=True).sum() / (1024**2)

#     # Downcast floats
#     for col in df.select_dtypes(include=['float64']).columns:
#         df[col] = df[col].astype(np.float32)

#     # Downcast integers
#     for col in df.select_dtypes(include=['int64']).columns:
#         if df[col].min() >= np.iinfo(np.int8).min and df[col].max() <= np.iinfo(np.int8).max:
#             df[col] = df[col].astype(np.int8)
#         elif df[col].min() >= np.iinfo(np.int16).min and df[col].max() <= np.iinfo(np.int16).max:
#             df[col] = df[col].astype(np.int16)
#         elif df[col].min() >= np.iinfo(np.int32).min and df[col].max() <= np.iinfo(np.int32).max:
#             df[col] = df[col].astype(np.int32)
#         # Else remain int64

#     df['ticker'] = df['ticker'].astype('category')
#     df['ticker_code'] = df['ticker'].cat.codes.astype(np.int16) # Use int16 for ticker codes

#     optimized_memory = df.memory_usage(deep=True).sum() / (1024**2)
#     print(f"  \u27A1 Memory usage: {initial_memory:.2f} MB \u27A1 {optimized_memory:.2f} MB (Reduced by {(initial_memory - optimized_memory)/initial_memory:.1%})")

#     # Rename target column
#     df = df.rename(columns={'y1': 'y'})

#     # Define features and target
#     # Define features and target (already done)
#     feature_cols = [c for c in df.columns if c not in ['timestamp', 'ticker', 'y']]

#     ### prova
#     print("\n\U0001F535 Step 2.5: Scaling delle features")
#     # Apply MinMaxScaler to numerical features (excluding 'ticker_code' if it's already an integer code)
#     # You need to fit the scaler *before* splitting into train/val if you use the generator directly.
#     # However, for correct train/val split scaling, you should scale *within* the generator or
#     # apply it to df_train/df_val before passing to the generator.

#     # Let's add scaling to the main workflow before the train/val split for simplicity for now.
#     # For production, fitting scaler ONLY on training data is critical.
#     # Here, as df is large, we'll fit on df.
#     # If you stick with the in-memory method (your current code), you'd fit scaler on X_seq_train.

#     # Assuming all feature_cols are numerical and need scaling
#     scaler = MinMaxScaler()
#     # Fit and transform the features of the *entire* DataFrame (for generator approach,
#     # ensure this is done properly on train/val split later).
#     # If you are using the generator, fitting on the whole df is acceptable if the data distribution
#     # is similar, but ideally, you'd fit on df_train ONLY.
#     df[feature_cols] = scaler.fit_transform(df[feature_cols])
#     print("\u2705 Features scalate tra 0 e 1 (MinMaxScaler)")
#     ### fine prova 
        
#     # --- Corrected Efficient Sequence Creation ---
#     print("\n\U0001F535 Step 3: Creazione Sequenze Ottimizzata")
#     X_seq_list, X_ticker_list, y_list = [], [], []

#     for ticker_code_val in df['ticker_code'].unique():
#         ticker_df = df[df['ticker_code'] == ticker_code_val].copy()

#         if len(ticker_df) >= N_STEPS:
#             ticker_features = ticker_df[feature_cols].values.astype(np.float32)
#             ticker_target = ticker_df['y'].values.astype(np.float32)
#             ticker_codes_original = ticker_df['ticker_code'].values.astype(np.int32) # Get original ticker_codes array

#             sequences = np.lib.stride_tricks.sliding_window_view(ticker_features, (N_STEPS, ticker_features.shape[1]))
#             sequences = sequences.squeeze(axis=1)

#             # The key fix: targets and ticker_codes should align with the output of sequences
#             # The sequences ending at index `i` (i.e., data from i-N_STEPS to i-1) predict the target at index `i`.
#             # So, the targets should be the 'y' values from N_STEPS up to the length of original data.
#             targets = ticker_target[N_STEPS:] # This is correct for the target being at the *next* step.
            
#             # We also need the ticker_code corresponding to the target (which is at index N_STEPS and onwards)
#             ticker_codes_for_sequences = ticker_codes_original[N_STEPS:]

#             # Ensure all arrays derived from a single ticker_df segment have the same length
#             # They should all have length `len(ticker_df) - N_STEPS`
            
#             # Let's confirm the length we expect from sequences:
#             expected_len = len(ticker_df) - N_STEPS # This is correct for the number of available predictions

#             # Double-check the length of 'sequences' for this ticker:
#             # sliding_window_view on an array of length L, with window size W, produces L - W + 1 windows.
#             # So, len(sequences) = len(ticker_df) - N_STEPS + 1. THIS IS THE SOURCE OF THE BUG.
#             # We need to trim sequences by 1.

#             # Corrected slicing for consistency:
#             # If sequences has N_rows - N_STEPS + 1 elements, and targets has N_rows - N_STEPS elements,
#             # we need to remove the first element from sequences to align them, OR
#             # remove the last element from sequences to align them.
#             # Let's remove the *last* element from sequences to align with targets starting from N_STEPS.
            
#             # Original: X_seq starts from first sequence (0 to N_STEPS-1) and ends at (L-N_STEPS to L-1)
#             #           y starts from index N_STEPS and ends at L-1
#             # This implies X_seq[0] corresponds to y[N_STEPS], X_seq[1] to y[N_STEPS+1] ...
#             # The length of y is `len(ticker_df) - N_STEPS`.
#             # The length of sequences (from sliding_window_view) is `len(ticker_df) - N_STEPS + 1`.
#             # So, `sequences` has one extra element. We need to match their lengths.
            
#             # Simplest fix is to take `sequences` from index 0 up to the length of `targets`.
#             # Or, alternatively, take targets and ticker_codes_for_sequences up to `len(sequences)`.
#             # The latter is usually safer if the target is truly for the *next* step.
            
#             # Let's align based on the number of targets.
#             # Sequences will have len(ticker_df) - N_STEPS + 1 entries
#             # Targets will have len(ticker_df) - N_STEPS entries.
#             # So we need to trim the sequences. The easiest is to drop the *first* sequence.
#             # This means sequence `0` (data `0..N_STEPS-1`) predicts `y[N_STEPS]`.
#             # sequence `1` (data `1..N_STEPS`) predicts `y[N_STEPS+1]`.
#             # This means `sequences[k]` predicts `y[N_STEPS + k]`.
            
#             # So, the number of samples in `sequences` should be `len(ticker_target) - N_STEPS`.
#             # The sliding_window_view gives us `len(ticker_target) - N_STEPS + 1`.
#             # Therefore, we need to drop the *last* sequence.
            
#             final_sequences = sequences[:-1] # Drop the very last sequence
#             final_targets = targets # Use targets as is
#             final_ticker_codes = ticker_codes_for_sequences # Use ticker_codes_for_sequences as is

#             # Append only if they all have consistent lengths
#             if len(final_sequences) == len(final_targets) == len(final_ticker_codes):
#                 X_seq_list.append(final_sequences)
#                 X_ticker_list.append(final_ticker_codes)
#                 y_list.append(final_targets)
#             else:
#                 print(f"  \u274C Warning: Inconsistent lengths for ticker_code {ticker_code_val} after slicing. Sequences: {len(final_sequences)}, Targets: {len(final_targets)}")

#         else:
#             print(f"  \u27A1 Skipping ticker_code {ticker_code_val} (not enough data for N_STEPS={N_STEPS})")

#     # The rest of your code (concatenation and train_test_split) remains the same.
#     # Concatenate data from all tickers
#     if not X_seq_list:
#         print("\u274C No sequences could be generated from the data. Check data quantity or N_STEPS.")
#         exit(1)

#     X_seq = np.concatenate(X_seq_list, axis=0)
#     X_ticker = np.concatenate(X_ticker_list, axis=0)
#     y = np.concatenate(y_list, axis=0)

#     print(f"\u2705 Sequenze create: X_seq={X_seq.shape}, X_ticker={X_ticker.shape}, y={y.shape}")
#     # Train/test split
#     # Stratify by ticker might be good for more even distribution of ticker data
#     # (requires y to be 1D for stratification, so using X_ticker here)
#     X_seq_train, X_seq_val, X_ticker_train, X_ticker_val, y_train, y_val = train_test_split(
#         X_seq, X_ticker, y, test_size=0.2, random_state=42, stratify=X_ticker if len(np.unique(X_ticker)) > 1 else None
#     )

#     # Convert to TensorFlow Datasets for efficient batching and prefetching
#     dataset_train = tf.data.Dataset.from_tensor_slices(((X_seq_train, X_ticker_train), y_train))
#     dataset_train = dataset_train.shuffle(1000).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

#     dataset_val = tf.data.Dataset.from_tensor_slices(((X_seq_val, X_ticker_val), y_val))
#     dataset_val = dataset_val.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)
#     print(f"\u2705 Dataset pronto: train={len(X_seq_train)} samples, val={len(X_seq_val)} samples")

#     print("\n\U0001F535 Step 4: Costruzione modello")
#     # Input layers
#     input_seq = layers.Input(shape=(N_STEPS, X_seq.shape[2]), name='input_seq')
#     input_ticker = layers.Input(shape=(), dtype='int32', name='input_ticker')

#     # Ticker embedding layer
#     num_unique_tickers = df['ticker_code'].nunique()
#     # output_dim should be chosen carefully; 4 is a reasonable starting point
#     ticker_embedding = layers.Embedding(input_dim=num_unique_tickers, output_dim=4)(input_ticker)
#     ticker_embedding = layers.RepeatVector(N_STEPS)(ticker_embedding) # Repeat for each step in the sequence

#     # Concatenate features and ticker embedding
#     x = layers.Concatenate()([input_seq, ticker_embedding])

#     # LSTM layers
#     x = layers.LSTM(64, return_sequences=False)(x) # return_sequences=False for single output prediction
#     x = layers.Dense(32, activation='relu')(x) # Dense layer for feature transformation
#     output = layers.Dense(1)(x) # Output layer for regression (single price prediction)

#     model = models.Model(inputs=[input_seq, input_ticker], outputs=output)
#     model.compile(optimizer='adam', loss='mse') # Adam optimizer, Mean Squared Error loss for regression
#     model.summary() # Print model summary
#     print("\u2705 Modello costruito")

#     print("\n\U0001F535 Step 5: Training")
#     # Train the model
#     history = model.fit(dataset_train, validation_data=dataset_val, epochs=EPOCHS)
#     print("\u2705 Training completato")

#     print("\n\U0001F535 Step 6: Salvataggio modello")
#     # Create directory if it doesn't exist
#     model_save_path = "model"
#     os.makedirs(model_save_path, exist_ok=True)
    
#     # Save the model
#     model_filename = os.path.join(model_save_path, "lstm_multi_ticker.h5")
#     model.save(model_filename)
#     print(f"\u2705 Modello multi-ticker salvato in {model_filename}")

#     print("\n--- Training Pipeline Completed ---")