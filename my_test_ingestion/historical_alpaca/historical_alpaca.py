from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import boto3

# === CONFIGURAZIONE ===
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
MINIO_BUCKET = os.getenv("S3_BUCKET", "historical-data")

START_DATE = datetime(2021, 1, 1)
END_DATE = datetime(2025, 5, 14)

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "LLY",
    "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO", "IBM"
]

def ensure_bucket_exists(bucket_name, endpoint, access_key, secret_key):
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    try:
        buckets = s3.list_buckets()
        if not any(b['Name'] == bucket_name for b in buckets.get('Buckets', [])):
            s3.create_bucket(Bucket=bucket_name)
            print(f"🪣 Bucket '{bucket_name}' creato.")
        else:
            print(f"✅ Bucket '{bucket_name}' già esistente.")
    except Exception as e:
        print(f"❌ Errore controllo/creazione bucket: {e}")


# === FUNZIONE PRINCIPALE ===
def download_and_save_ticker_data(ticker, spark):
    print(f"⬇️  Scarico dati per {ticker}...")
    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    req = StockBarsRequest(
        symbol_or_symbols=ticker,
        timeframe=TimeFrame.Hour,
        start=START_DATE,
        end=END_DATE
    )
    try:
        bars = client.get_stock_bars(req).df
        if bars.empty:
            print(f"⚠️  Nessun dato per {ticker}")
            return

        bars = bars.reset_index()

        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", FloatType(), True),
            StructField("trade_count", FloatType(), True),
            StructField("vwap", FloatType(), True),
        ])

        df = spark.createDataFrame(bars, schema=schema)
        df = df.withColumn("date", to_date(col("timestamp")))

        dates = [row["date"].strftime("%Y-%m-%d") for row in df.select("date").distinct().collect()]

        for date_str in dates:
            df_day = df.filter(col("date") == date_str).drop("date")
            year_val = int(date_str[:4])
            month_val = int(date_str[5:7])

            output_path = f"s3a://{MINIO_BUCKET}/{ticker}/year={year_val}/month={str(month_val).zfill(2)}/{ticker}_{date_str}.parquet"

            df_day.write.mode("overwrite").parquet(output_path)
            print(f"✅ Salvato: {output_path}")

    except Exception as e:
        print(f"❌ Errore su {ticker}: {e}")

# === MAIN ===
if __name__ == "__main__":
    ensure_bucket_exists(
        bucket_name=MINIO_BUCKET,
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    spark = SparkSession.builder \
        .appName("AlpacaToMinIO") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    for ticker in TICKERS:
        download_and_save_ticker_data(ticker, spark)

    spark.stop()
