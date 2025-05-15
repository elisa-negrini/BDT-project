from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import boto3
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from multiprocessing import cpu_count

# === CONFIGURAZIONE ===
ALPACA_API_KEY = "AKALIZA66I0M7HGJVNTO"
ALPACA_SECRET_KEY = "k5hw9IxWpIP3WCNbzuo2I7NegamWamUX0nFujWdN"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "historical-data"

START_DATE = datetime(2021, 1, 1)
END_DATE = datetime(2025, 5, 14)

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "LLY",
    "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO", "CRM"
]

# === CLIENT MINIO ===
s3 = boto3.client(
    's3',
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# === CREA BUCKET SE NON ESISTE ===
def ensure_bucket(bucket_name):
    existing_buckets = [bucket['Name'] for bucket in s3.list_buckets().get('Buckets', [])]
    if bucket_name not in existing_buckets:
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' creato.")
    else:
        print(f"ℹ️  Bucket '{bucket_name}' già esistente.")

# === UPLOAD FILE SU MINIO ===
def upload_to_minio(local_path, remote_path):
    s3.upload_file(
        Filename=local_path,
        Bucket=MINIO_BUCKET,
        Key=remote_path
    )
    print(f"✅ Caricato su MinIO: {remote_path}")

# === FUNZIONE DI DOWNLOAD E UPLOAD PER TICKER ===
def download_and_upload_ticker_data(ticker, spark):
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

        spark_df = spark.createDataFrame(bars, schema=schema)
        spark_df = spark_df.withColumn("date", to_date(col("timestamp")))

        dates = [row["date"].strftime("%Y-%m-%d") for row in spark_df.select("date").distinct().collect()]

        for date in dates:
            daily_df = spark_df.filter(col("date") == date).drop("date")
            year_month = date[:7]

            local_parquet_dir = os.path.join("tmp", ticker, year_month)
            os.makedirs(local_parquet_dir, exist_ok=True)
            local_parquet_path = os.path.join(local_parquet_dir, f"{date}.parquet")

            daily_df.write.mode("overwrite").parquet(local_parquet_path)

            remote_path = f"{ticker}/{year_month}/{date}.parquet"
            upload_to_minio(local_parquet_path, remote_path)

    except Exception as e:
        print(f"❌ Errore su {ticker}: {e}")

# === MAIN ===
if __name__ == "__main__":
    ensure_bucket(MINIO_BUCKET)

    spark = SparkSession.builder \
    .appName("AlpacaToMinIO") \
    .master(f"local[{cpu_count()}]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.pyspark.python", sys.executable) \
    .config("spark.pyspark.driver.python", sys.executable) \
    .getOrCreate()

    for ticker in TICKERS:
        download_and_upload_ticker_data(ticker, spark)

    spark.stop()