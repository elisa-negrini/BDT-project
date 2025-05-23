# import os
# import time
# import json
# import requests
# import boto3
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, year, lit, month
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# # === Config ===
# API_KEY = "119c5415679f73cb0da3b62e9c2a534d"
# BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
# S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
# S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'admin')
# S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'admin123')
# S3_BUCKET = os.getenv('S3_BUCKET', 'macro-data')

# series_dict = {
#     "GDPC1": "gdp_real",
#     "CPIAUCSL": "cpi",
#     "FEDFUNDS": "ffr",
#     "DGS10": "t10y",
#     "DGS2": "t2y",
#     "T10Y2Y": "spread_10y_2y",
#     "UNRATE": "unemployment",
# }

# # === Ensure S3 Bucket ===
# def ensure_bucket_exists():
#     s3 = boto3.client(
#         's3',
#         endpoint_url=S3_ENDPOINT,
#         aws_access_key_id=S3_ACCESS_KEY,
#         aws_secret_access_key=S3_SECRET_KEY
#     )
#     try:
#         buckets = s3.list_buckets()
#         if not any(b['Name'] == S3_BUCKET for b in buckets['Buckets']):
#             s3.create_bucket(Bucket=S3_BUCKET)
#             print(f"🪣 Bucket '{S3_BUCKET}' creato.")
#         else:
#             print(f"✅ Bucket '{S3_BUCKET}' già esistente.")
#     except Exception as e:
#         print(f"❌ Errore nella verifica/creazione del bucket: {e}")

# # === Spark Session ===
# spark = SparkSession.builder \
#     .appName("FRED MacroData Ingestion") \
#     .master("spark://spark-master:7077") \
#     .config("spark.executor.cores", "1") \
#     .config("spark.cores.max", "1") \
#     .config("spark.executor.memory", "1g") \
#     .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
#     .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
#     .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# # Schema
# schema = StructType([
#     StructField("Date", StringType(), True),
#     StructField("Value", DoubleType(), True)
# ])

# # === Funzione di salvataggio ===

# def save_to_s3(alias, df_spark):
#     output_base = f"s3a://{S3_BUCKET}/{alias}"

#     try:
#         # Aggiungi colonna 'year' per costruire il path
#         df_spark = df_spark.withColumn("year", year("Date"))

#         # Estrai tutte le date presenti (una per giorno)
#         unique_dates = [row["Date"] for row in df_spark.select("Date").distinct().collect()]

#         for date_str in unique_dates:
#             # Filtro per la singola data
#             df_day = df_spark.filter(col("Date") == date_str)

#             # Estrai l'anno corrispondente
#             year_val = df_day.select("year").first()["year"]

#             # Costruisci il nome leggibile del file
#             filename = f"{alias}_{date_str}.parquet"
#             output_path = f"{output_base}/year={year_val}/{filename}"

#             # Scrivi il file (senza la colonna 'year')
#             df_day.drop("year").write.mode("overwrite").parquet(output_path)

#             print(f"✅ Salvato: {output_path}")
#     except Exception as e:
#         print(f"❌ Errore salvataggio {alias}: {e}")


# # === Fetch & Save ===
# def fetch_and_save_all():
#     for series_id, alias in series_dict.items():
#         url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json"
#         print(f"⬇️  Scarico {alias}...")
#         try:
#             response = requests.get(url)
#             if response.status_code == 200:
#                 data = response.json().get("observations", [])
#                 records = [
#                     {"Date": obs["date"], "Value": float(obs["value"])}
#                     for obs in data if obs["value"] != "."
#                 ]
#                 if records:
#                     df_pd = spark.createDataFrame(records, schema=schema)
#                     save_to_s3(alias, df_pd)
#                 else:
#                     print(f"⚠️ Nessun dato valido per {alias}")
#             else:
#                 print(f"❌ Errore FRED per {series_id}: status {response.status_code}")
#         except Exception as e:
#             print(f"❌ Errore richiesta {series_id}: {e}")

# if __name__ == "__main__":
#     print("🚀 Inizio Ingestione con Spark...")
#     ensure_bucket_exists()
#     fetch_and_save_all()
#     spark.stop()

# print("FINISH!!!")

import json
import requests
from kafka import KafkaProducer

API_KEY = "119c5415679f73cb0da3b62e9c2a534d"
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
KAFKA_TOPIC = "macro-data"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

series_dict = {
    "GDPC1": "gdp_real",
    "CPIAUCSL": "cpi",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment",
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_send():
    for series_id, alias in series_dict.items():
        url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json"
        print(f"⬇️  Scarico {alias}...")
        try:
            response = requests.get(url)
            if response.status_code == 200:
                observations = response.json().get("observations", [])
                for obs in observations:
                    if obs["value"] != ".":
                        record = {
                            "series": alias,
                            "date": obs["date"],
                            "value": float(obs["value"])
                        }
                        producer.send(KAFKA_TOPIC, record)
                print(f"✅ Dati inviati per {alias}")
            else:
                print(f"❌ Errore FRED {series_id}: {response.status_code}")
        except Exception as e:
            print(f"❌ Errore {series_id}: {e}")

if __name__ == "__main__":
    fetch_and_send()
    producer.flush()
    producer.close()



