# da scaricare: pip install pyflink pandas s3fs ta holidays pyarrow
# requisito di sistema: sudo apt install openjdk-17-jdk

# === IMPORT LIBRERIE ===
import os
import pandas as pd
import numpy as np
import s3fs
from datetime import datetime
import holidays

# === CARICA VARIABILI DA .env ===
from dotenv import load_dotenv
load_dotenv()  # Cerca il file .env nella root e carica le variabili

# Indicatori tecnici (RSI, EMA, MACD, Bollinger)
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, MACD
from ta.volatility import BollingerBands

# Flink APIs
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col

# === SETUP FLINK ENVIRONMENT ===
# Usiamo Flink in modalità batch per processare un blocco di dati storici
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# === CONFIGURAZIONE ACCESSO A MINIO (via S3A) ===
# Si specificano le credenziali e l'endpoint del bucket MinIO
s3_endpoint = "http://minio:9000"
s3_key = os.getenv("MINIO_ACCESS_KEY")
s3_secret = os.getenv("MINIO_SECRET_KEY")

conf = t_env.get_config().get_configuration()
conf.set_string("fs.s3a.endpoint", s3_endpoint)
conf.set_string("fs.s3a.access.key", s3_key)
conf.set_string("fs.s3a.secret.key", s3_secret)
conf.set_string("fs.s3a.path.style.access", "true")
conf.set_string("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# === CARICAMENTO DATI STOCK ===
# La tabella stock contiene: timestamp, ticker, price, size
t_env.execute_sql("""
    CREATE TEMPORARY TABLE stock_data (
        `timestamp` TIMESTAMP(3),
        ticker STRING,
        price DOUBLE,
        size BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 's3a://stock-data/',
        'format' = 'parquet'
    )
""")
stock_table = t_env.from_path("stock_data")

# Aggiungiamo la colonna "date" per poter fare il join con i dati macro
stock_table = stock_table.add_columns(col("timestamp").cast("DATE").alias("date"))

# === FUNZIONE PER CARICARE I DATI MACRO DA MINIO ===
def load_macro_from_minio():
    fs = s3fs.S3FileSystem(
        key=s3_key,
        secret=s3_secret,
        client_kwargs={"endpoint_url": s3_endpoint}
    )

    # Lista di variabili macroeconomiche da includere
    macro_vars = [
        "GDP", "GDPC1", "QoQ", "CPI", "Fed_Funds_Rate",
        "Treasury_10Y", "Treasury_2Y", "Yield_Spread", "Unemployment_Rate"
    ]

    macro_dfs = []
    for var in macro_vars:
        # Legge tutti i parquet del tipo: macro-data/<var>/<anno>/<mese>/file.parquet
        files = fs.glob(f"macro-data/{var}/*/*/*.parquet")
        var_df = pd.concat([
            pd.read_parquet(f"s3://{f}", filesystem=fs).assign(variable=var)
            for f in files
        ], ignore_index=True)
        macro_dfs.append(var_df)

    # Concatenazione e pivot: una riga al giorno con tutte le variabili
    all_macro = pd.concat(macro_dfs)
    all_macro["timestamp"] = pd.to_datetime(all_macro["timestamp"])
    macro_wide = all_macro.pivot(index="timestamp", columns="variable", values="value")
    macro_wide = macro_wide.sort_index().resample("D").ffill().reset_index()
    macro_wide["date"] = macro_wide["timestamp"].dt.date
    macro_wide.drop(columns=["timestamp"], inplace=True)
    return macro_wide

# Salvataggio dei macro dati in file temporaneo per poterli leggere con Flink
macro_df = load_macro_from_minio()
macro_df.to_parquet("macro_temp.parquet", index=False)

# === REGISTRAZIONE TABELLA MACRO ===
t_env.execute_sql("""
    CREATE TEMPORARY TABLE macro_data (
        date DATE,
        GDP DOUBLE,
        GDPC1 DOUBLE,
        QoQ DOUBLE,
        CPI DOUBLE,
        Fed_Funds_Rate DOUBLE,
        Treasury_10Y DOUBLE,
        Treasury_2Y DOUBLE,
        Yield_Spread DOUBLE,
        Unemployment_Rate DOUBLE
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'macro_temp.parquet',
        'format' = 'parquet'
    )
""")
macro_table = t_env.from_path("macro_data")

# === JOIN STOCK + MACRO SU 'date' ===
enriched_table = stock_table.join(macro_table)

# === CONVERSIONE A PANDAS PER PREPROCESSING AVANZATO ===
df = t_env.to_pandas(enriched_table)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values(["ticker", "timestamp"]).set_index("timestamp")

# === FEATURE ENGINEERING PER TICKER ===
def compute_features(group):
    df = group.copy()
    
    # Medie mobili, ritorni logaritmici, volatilità
    df["price_mean_1min"] = df["price"]
    df["price_mean_5min"] = df["price"].rolling(window=5).mean()
    df["log_return_5min"] = np.log(df["price"] / df["price"].shift(5))
    df["price_std_15min"] = df["price"].rolling("15min").std()
    df["price_max_30min"] = df["price"].rolling("30min").max()

    # Volume e variazioni
    df["Size_1min"] = df["size"]
    df["Size_diff_1min"] = df["size"].diff()
    df["Size_mean_15min"] = df["size"].rolling("15min").mean()
    df["Size_std_15min"] = df["size"].rolling("15min").std()

    # Indicatori tecnici
    df["rsi_14"] = RSIIndicator(close=df["price"], window=14).rsi()
    df["ema_10"] = EMAIndicator(close=df["price"], window=10).ema_indicator()
    df["macd_line"] = MACD(close=df["price"]).macd()
    bb = BollingerBands(close=df["price"])
    df["bollinger_width"] = bb.bollinger_hband() - bb.bollinger_lband()
    df["obv"] = (np.sign(df["price"].diff()) * df["size"]).fillna(0).cumsum()

    # Feature temporali legate al mercato
    df["minutes_since_open"] = (df.index.time >= datetime.strptime("09:30", "%H:%M").time()) * (
        (df.index.hour * 60 + df.index.minute) - 570
    )
    df["sin_minute"] = np.sin(2 * np.pi * df["minutes_since_open"] / 390)
    df["cos_minute"] = np.cos(2 * np.pi * df["minutes_since_open"] / 390)
    df["day_of_week"] = df.index.dayofweek
    df["day_of_month"] = df.index.day
    df["week_of_year"] = df.index.isocalendar().week.astype(int)
    df["month_of_year"] = df.index.month
    df["is_month_end"] = df.index.is_month_end.astype(int)
    df["is_quarter_end"] = df.index.is_quarter_end.astype(int)

    # Distanza da festività (es. Natale)
    us_holidays = holidays.US()
    df["holiday_proximity"] = df.index.to_series().apply(
        lambda d: min([(abs((d.date() - h).days)) for h in us_holidays if abs((d.date() - h).days) <= 5] + [5])
    )
    
    # Flag aperture/chiusure mercato
    df["market_open_spike_flag"] = ((df.index.time >= datetime.strptime("09:30", "%H:%M").time()) &
                                    (df.index.time < datetime.strptime("09:35", "%H:%M").time())).astype(int)
    df["market_close_spike_flag"] = ((df.index.time >= datetime.strptime("15:55", "%H:%M").time()) &
                                     (df.index.time < datetime.strptime("16:00", "%H:%M").time())).astype(int)
    return df

# Applica tutte le trasformazioni per ogni ticker
features_df = df.groupby("ticker", group_keys=False).apply(compute_features)

# === SALVA IL RISULTATO COMPLETO SU MINIO ===
features_df.reset_index().to_parquet(
    "s3a://processed-data/features_alpaca_ready.parquet",
    engine="pyarrow",
    storage_options={
        "key": s3_key,
        "secret": s3_secret,
        "client_kwargs": {"endpoint_url": s3_endpoint}
    }
)

print("✅ Feature engineering completa. File scritto su MinIO.")
