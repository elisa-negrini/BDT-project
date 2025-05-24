


# from alpaca.data.historical import StockHistoricalDataClient
# from alpaca.data.requests import StockBarsRequest
# from alpaca.data.timeframe import TimeFrame
# from datetime import datetime
# import os
# import sys
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
# import boto3

# # === CONFIGURAZIONE ===
# ALPACA_API_KEY = "PK2SQFP2LZZSNJDBHY31"
# ALPACA_SECRET_KEY = "sw6g8o3Pm3HQ3TPufGCGxKBKSAYqQWS1lgN9Qhit"

# MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
# MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
# MINIO_BUCKET = os.getenv("S3_BUCKET", "historical-data")

# START_DATE = datetime(2021, 1, 1)
# END_DATE = datetime(2025, 5, 14)

# TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "LLY",
#     "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO", "IBM"
# ]

# def ensure_bucket_exists(bucket_name, endpoint, access_key, secret_key):
#     s3 = boto3.client(
#         's3',
#         endpoint_url=endpoint,
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key
#     )
#     try:
#         buckets = s3.list_buckets()
#         if not any(b['Name'] == bucket_name for b in buckets.get('Buckets', [])):
#             s3.create_bucket(Bucket=bucket_name)
#             print(f"🪣 Bucket '{bucket_name}' creato.")
#         else:
#             print(f"✅ Bucket '{bucket_name}' già esistente.")
#     except Exception as e:
#         print(f"❌ Errore controllo/creazione bucket: {e}")

# # === FUNZIONE PRINCIPALE ===
# def download_and_save_ticker_data(ticker, spark):
#     print(f"⬇️  Scarico dati per {ticker}...")
#     client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
#     req = StockBarsRequest(
#         symbol_or_symbols=ticker,
#         timeframe=TimeFrame.Minute,  # Cambiato da Hour a Minute
#         start=START_DATE,
#         end=END_DATE
#     )
#     try:
#         bars = client.get_stock_bars(req).df
#         if bars.empty:
#             print(f"⚠️  Nessun dato per {ticker}")
#             return

#         bars = bars.reset_index()

#         schema = StructType([
#             StructField("symbol", StringType(), True),
#             StructField("timestamp", TimestampType(), True),
#             StructField("open", FloatType(), True),
#             StructField("high", FloatType(), True),
#             StructField("low", FloatType(), True),
#             StructField("close", FloatType(), True),
#             StructField("volume", FloatType(), True),
#             StructField("trade_count", FloatType(), True),
#             StructField("vwap", FloatType(), True),
#         ])

#         df = spark.createDataFrame(bars, schema=schema)
#         df = df.withColumn("date", to_date(col("timestamp")))

#         # Un parquet al giorno
#         dates = [row["date"].strftime("%Y-%m-%d") for row in df.select("date").distinct().collect()]
#         for date_str in dates:
#             df_day = df.filter(col("date") == date_str).drop("date")
#             year_val = int(date_str[:4])
#             month_val = int(date_str[5:7])

#             output_path = f"s3a://{MINIO_BUCKET}/{ticker}/year={year_val}/month={str(month_val).zfill(2)}/{ticker}_{date_str}.parquet"
#             df_day.write.mode("overwrite").parquet(output_path)
#             print(f"✅ Salvato: {output_path}")

#     except Exception as e:
#         print(f"❌ Errore su {ticker}: {e}")

# # === MAIN ===
# if __name__ == "__main__":
#     ensure_bucket_exists(
#         bucket_name=MINIO_BUCKET,
#         endpoint=MINIO_ENDPOINT,
#         access_key=MINIO_ACCESS_KEY,
#         secret_key=MINIO_SECRET_KEY
#     )

#     spark = SparkSession.builder \
#         .appName("AlpacaToMinIO") \
#         .master("local[*]") \
#         .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
#         .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
#         .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .getOrCreate()

#     for ticker in TICKERS:
#         download_and_save_ticker_data(ticker, spark)

#     spark.stop()


#!/usr/bin/env python3
"""
Producer: Scarica dati da Alpaca e li invia a Kafka
"""

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, date
import json
import os
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import logging

# === CONFIGURAZIONE ===
ALPACA_API_KEY = "PKJ6P4HODXR8JH8NDJQB"
ALPACA_SECRET_KEY = "kjCxxZzuoeIl8YmusdW7Q1xYZaJqodZoxOMwNBTr"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "historical_alpaca")

START_DATE = datetime(2021, 1, 1)
END_DATE = datetime(2025, 5, 14)

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "LLY",
    "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO", "IBM"
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """Encoder personalizzato per gestire datetime e date"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return super().default(obj)

def create_kafka_producer():
    """Crea e configura il producer Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Attendere conferma da tutti i replica
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        logger.info(f"✅ Producer Kafka connesso a {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"❌ Errore connessione Kafka: {e}")
        sys.exit(1)

def download_ticker_data(ticker):
    """Scarica dati per un singolo ticker da Alpaca"""
    logger.info(f"⬇️  Scarico dati per {ticker}...")
    
    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    req = StockBarsRequest(
        symbol_or_symbols=ticker,
        timeframe=TimeFrame.Minute,
        start=START_DATE,
        end=END_DATE
    )
    
    try:
        bars = client.get_stock_bars(req).df
        if bars.empty:
            logger.warning(f"⚠️  Nessun dato per {ticker}")
            return None

        bars = bars.reset_index()
        logger.info(f"📊 Scaricati {len(bars)} record per {ticker}")
        return bars

    except Exception as e:
        logger.error(f"❌ Errore scaricamento {ticker}: {e}")
        return None

def send_data_to_kafka(producer, ticker, df):
    """Invia i dati del ticker a Kafka, raggruppati per giorno"""
    if df is None or df.empty:
        return

    # Aggiungi colonna date per raggruppamento
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    
    # Raggruppa per data
    grouped = df.groupby('date')
    
    for date_val, group_df in grouped:
        # Prepara il messaggio
        message = {
            'ticker': ticker,
            'date': date_val.isoformat(),
            'year': date_val.year,
            'month': date_val.month,
            'data': group_df.drop('date', axis=1).to_dict('records')
        }
        
        try:
            # Invia a Kafka con chiave = ticker_data
            future = producer.send(
                topic=KAFKA_TOPIC,
                key=f"{ticker}_{date_val}",
                value=message
            )
            
            # Attendi conferma (opzionale per debugging)
            future.get(timeout=10)
            logger.info(f"📤 Inviato {ticker} per {date_val} ({len(group_df)} record)")
            
        except KafkaError as e:
            logger.error(f"❌ Errore invio Kafka per {ticker} {date_val}: {e}")
        except Exception as e:
            logger.error(f"❌ Errore generico per {ticker} {date_val}: {e}")

def main():
    """Funzione principale del producer"""
    logger.info("🚀 Avvio Producer Alpaca -> Kafka")
    
    # Crea producer Kafka
    producer = create_kafka_producer()
    
    try:
        processed_count = 0
        error_count = 0
        
        for ticker in TICKERS:
            try:
                # Scarica dati
                df = download_ticker_data(ticker)
                
                if df is not None:
                    # Invia a Kafka
                    send_data_to_kafka(producer, ticker, df)
                    processed_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"❌ Errore elaborazione {ticker}: {e}")
                error_count += 1
        
        # Assicurati che tutti i messaggi siano inviati
        producer.flush()
        
        logger.info(f"✅ Completato! Processati: {processed_count}, Errori: {error_count}")
        
    except KeyboardInterrupt:
        logger.info("⏹️  Interruzione da utente")
    except Exception as e:
        logger.error(f"❌ Errore fatale: {e}")
    finally:
        producer.close()
        logger.info("🔚 Producer chiuso")

if __name__ == "__main__":
    main()