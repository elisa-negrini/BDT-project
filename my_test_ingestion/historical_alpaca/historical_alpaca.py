from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import os

# === ENV ===
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "PKCAB7RKX3K5IH490XM6")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "j0t7VFg1B1Ew2vzUIPe4GssT7rAkmvxXpczi5htP")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
S3_BUCKET = "historical"

# === SparkSession con accesso a MinIO via S3A ===
spark = SparkSession.builder \
    .appName("AlpacaStockDataIngest") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# === Lista ticker S&P Top 30 ===
top_30_tickers = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

# === Scarica i dati da Alpaca ===
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

request_params = StockBarsRequest(
    symbol_or_symbols=top_30_tickers,
    timeframe=TimeFrame.Day,
    start=datetime(2021, 4, 23),
    end=datetime.(2025, 5, 14)
)

print("⬇️  Scarico i dati storici da Alpaca...")
bars = client.get_stock_bars(request_params)
df = bars.df.reset_index()

# === Conversione a Spark DataFrame ===
spark_df = spark.createDataFrame(df)

# === Estrai colonne di partizione: ticker e giorno ===
spark_df = spark_df \
    .withColumn("date", to_date("timestamp")) \
    .withColumn("day", date_format(col("date"), "yyyy-MM-dd")) \
    .withColumnRenamed("symbol", "ticker")

# === Scrittura su MinIO in path: s3a://historical/stock_data/ticker=XXX/day=YYYY-MM-DD/part-....parquet ===
output_path = f"s3a://{S3_BUCKET}/stock_data"

spark_df.write \
    .mode("overwrite") \
    .partitionBy("ticker", "day") \
    .format("parquet") \
    .save(output_path)

print("✅ Dati salvati su MinIO partizionati per ticker e giorno.")
