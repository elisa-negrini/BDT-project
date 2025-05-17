from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max
from datetime import datetime, timedelta
import os

# === ENV ===
LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "/app")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
S3_BUCKET = "historical"

# === Spark session ===
spark = SparkSession.builder \
    .appName("UploadToMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# === Leggi tutti i file parquet ===
parquet_files = [f for f in os.listdir(LOCAL_DATA_DIR) if f.endswith(".parquet")]

if not parquet_files:
    print("⚠️ Nessun file .parquet trovato nella directory:", LOCAL_DATA_DIR)

for file in parquet_files:
    ticker = file.replace(".parquet", "")
    file_path = os.path.join(LOCAL_DATA_DIR, file)

    print(f"\n📥 Carico {file_path} (ticker={ticker})...")

    try:
        df = spark.read.parquet(file_path)

        if 'timestamp' not in df.columns:
            print(f"⚠️ Colonna 'timestamp' mancante in {file}, salto.")
            continue

        df = df.withColumn("ts", col("timestamp").cast("timestamp"))
        total_rows = df.count()
        print(f"📊 {total_rows} righe totali per {ticker}")

        if total_rows == 0:
            continue

        min_ts = df.select(spark_min("ts")).first()[0]
        max_ts = df.select(spark_max("ts")).first()[0]

        if not min_ts or not max_ts:
            continue

        current_day = datetime(min_ts.year, min_ts.month, min_ts.day)
        last_day = datetime(max_ts.year, max_ts.month, max_ts.day)

        while current_day <= last_day:
            next_day = current_day + timedelta(days=1)
            day_str = current_day.strftime("%Y-%m-%d")
            month_str = current_day.strftime("%Y-%m")

            df_day = df.filter((col("ts") >= day_str) & (col("ts") < next_day.strftime("%Y-%m-%d")))
            count = df_day.count()

            if count == 0:
                current_day = next_day
                continue

            output_path = f"s3a://{S3_BUCKET}/stock_data/ticker={ticker}/month={month_str}/"
            print(f"📝 Scrivo {count} righe per {day_str} → {output_path}")

            print("⏳ Scrittura in corso...")
            df_day.drop("ts").write \
                .mode("append") \
                .format("parquet") \
                .save(output_path)
            print("✅ Scrittura completata")

            spark.catalog.clearCache()  # 🔄 forza flush e pulizia cache
            print(f"✅ File scritto per {day_str}")

            current_day = next_day

        print(f"🏁 Completato per {ticker}")

    except Exception as e:
        print(f"❌ Errore durante il processing di {ticker}: {e}")
