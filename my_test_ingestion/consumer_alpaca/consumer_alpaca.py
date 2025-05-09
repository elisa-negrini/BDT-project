import json
import pandas as pd
import s3fs
import time
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# === Config ===
KAFKA_TOPIC = 'stock_trades'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'

S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'admin'
S3_SECRET_KEY = 'admin123'
S3_BUCKET = 'stock-data'

# === Retry Kafka connection ===
for _ in range(10):
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='stock-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("✓ Connesso a Kafka")
        break
    except NoBrokersAvailable:
        print("⚠️ Kafka non disponibile, riprovo tra 5s...")
        time.sleep(5)
else:
    raise RuntimeError("❌ Impossibile connettersi a Kafka dopo 10 tentativi.")


# === MinIO via s3fs ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT}
)

# ✅ QUI inserisci la funzione
import pyarrow.parquet as pq

def read_parquet_single_file(s3_dir_path, fs):
    files = fs.ls(s3_dir_path)
    parquet_files = [f for f in files if f.endswith('.parquet')]
    if not parquet_files:
        raise FileNotFoundError("No Parquet file found")
    
    # ⚠️ Usa il file esatto, NON la cartella
    # Es: minio/stock-data/stock/ticker=AAPL/date=2025-05-09/stock.parquet
    first_file_path = parquet_files[0]  # già include il prefisso s3://
    table = pq.read_table(first_file_path, filesystem=fs)
    return table.to_pandas()


# Buffer
buffer = []
last_flush = time.time()

FLUSH_INTERVAL = 30  # seconds

while True:
    message = next(consumer)
    data = message.value

    row = {
        "Ticker": data["Ticker"],
        "Timestamp": pd.to_datetime(data["Timestamp"], utc=True).isoformat(),
        "Price": data["Price"],
        "Size": data["Size"],
        "Exchange": data["Exchange"]
    }
    buffer.append(row)

    if time.time() - last_flush > FLUSH_INTERVAL:
        df = pd.DataFrame(buffer)
        df['date'] = pd.to_datetime(df['Timestamp']).dt.date.astype(str)

        for (ticker, date), group in df.groupby(['Ticker', 'date']):
            path = f"{S3_BUCKET}/stock/ticker={ticker}/date={date}/"
            s3_dir_path = f"s3://{path}"


            # Try to read existing file (if it exists)
            try:
                existing = read_parquet_single_file(s3_dir_path, fs)
                existing["Ticker"] = existing["Ticker"].astype(str)
                existing["Exchange"] = existing["Exchange"].astype(str)
                group = pd.concat([existing, group], ignore_index=True)
                group = group.drop_duplicates(subset=['Timestamp'])
            except FileNotFoundError:
                pass  # No file yet

            group.drop(columns='date').to_parquet(
                f"{s3_dir_path}stock.parquet",
                engine='pyarrow',
                filesystem=fs,
                index=False,
                use_dictionary={'Ticker': False, 'Exchange': False}
            )
            print(f"✓ Dati aggiornati per {ticker} {date}")

        buffer = []
        last_flush = time.time()