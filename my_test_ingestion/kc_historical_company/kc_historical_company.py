# #!/usr/bin/env python3
# import os
# import json
# from datetime import datetime
# from kafka import KafkaConsumer
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, FloatType
# from pyspark.sql.functions import col

# # === CONFIG ===
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "historical_company")
# MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
# MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
# MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
# MINIO_BUCKET = os.getenv("S3_BUCKET", "company-fundamentals")

# # === SPARK SESSION ===
# spark = SparkSession.builder \
#     .appName("CompanyConsumer") \
#     .master("local[*]") \
#     .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
#     .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
#     .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # === KAFKA CONSUMER ===
# consumer = KafkaConsumer(
#     KAFKA_TOPIC,
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     consumer_timeout_ms=10000
# )

# print("👂 In ascolto su Kafka...")

# records = []
# for message in consumer:
#     records.append(message.value)
#     print(f"📥 Ricevuto {message.value.get('symbol')}")

# if not records:
#     print("⚠️ Nessun dato ricevuto.")
#     exit(0)

# # === CONVERT TO SPARK DF ===
# schema = StructType([
#     StructField("symbol", StringType(), True),
#     StructField("calendarYear", StringType(), True),
#     # Aggiungi altri campi specifici del tuo DataFrame
# ])

# df = spark.createDataFrame(records, schema=schema)

# # === WRITE TO MINIO ===
# df = df.filter(col("calendarYear").isNotNull())
# df.write.partitionBy("symbol", "calendarYear").mode("overwrite").parquet(f"s3a://{MINIO_BUCKET}/")

# print("✅ Dati salvati su MinIO")
# consumer.close()
# spark.stop()



#!/usr/bin/env python3
import os
import json
import time
import pandas as pd
import boto3
from io import BytesIO
from kafka import KafkaConsumer
import logging

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaToMinio")

# === CONFIG ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "h_company")
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
MINIO_BUCKET = os.getenv("S3_BUCKET", "company-fundamentals")

# === FUNZIONE CONSUMER KAFKA ===
def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=10000
            )
            logger.info("✅ Connessione a Kafka riuscita (consumer).")
            return consumer
        except Exception as e:
            logger.warning(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
            time.sleep(5)

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
            logger.info(f"🪣 Bucket '{bucket_name}' creato.")
        else:
            logger.info(f"✅ Bucket '{bucket_name}' già esistente.")
    except Exception as e:
        logger.error(f"❌ Errore controllo/creazione bucket: {e}")
        raise

# === AVVIA CONSUMER ===
consumer = connect_kafka_consumer()
logger.info("👂 In ascolto su Kafka...")

records = []
for message in consumer:
    records.append(message.value)
    logger.info(f"📥 Ricevuto {message.value.get('symbol')}")

if not records:
    logger.warning("⚠️ Nessun dato ricevuto.")
    exit(0)

# === CONVERT TO PANDAS DF ===
df = pd.DataFrame(records)
df = df[df['calendarYear'].notna()]

required_columns = {'symbol', 'calendarYear'}
if not required_columns.issubset(df.columns):
    logger.error(f"❌ Colonne mancanti nel DataFrame: {required_columns - set(df.columns)}")
    exit(1)

# === WRITE TO MINIO ===
ensure_bucket_exists(MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

for (symbol, year), group_df in df.groupby(['symbol', 'calendarYear']):
    buffer = BytesIO()
    group_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    key = f"{symbol}/{year}/data.parquet"
    s3.upload_fileobj(buffer, MINIO_BUCKET, key)
    logger.info(f"✅ Caricato: {key}")

consumer.close()
logger.info("✅ Dati salvati su MinIO")

