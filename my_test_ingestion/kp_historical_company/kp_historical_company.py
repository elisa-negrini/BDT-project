# import pandas as pd
# import os
# import io
# import pyarrow as pa
# import pyarrow.parquet as pq
# from minio import Minio
# from minio.error import S3Error
# import time

# # === CONFIG ===
# MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
# BUCKET_NAME = "company-fundamentals"

# # === CONNECT TO MINIO ===
# client = Minio(
#     MINIO_URL,
#     access_key=MINIO_ACCESS_KEY,
#     secret_key=MINIO_SECRET_KEY,
#     secure=False
# )

# # === ATTENDI MinIO (loop infinito finché non parte) ===
# while True:
#     try:
#         if not client.bucket_exists(BUCKET_NAME):
#             client.make_bucket(BUCKET_NAME)
#             print(f"✓ Creato bucket '{BUCKET_NAME}'")
#         else:
#             print(f"✓ Bucket '{BUCKET_NAME}' già esistente")
#         break
#     except Exception as e:
#         print(f"⏳ In attesa che MinIO sia pronto... {e}")
#         time.sleep(5)


# # === READ DATAFRAME ===
# file_path = "/app/historical_company/df_company_fundamentals.parquet"
# df = pd.read_parquet(file_path)

# # === CREATE BUCKET IF NOT EXISTS ===
# if not client.bucket_exists(BUCKET_NAME):
#     client.make_bucket(BUCKET_NAME)
#     print(f"✓ Create bucket '{BUCKET_NAME}'")
# else:
#     print(f"✓ Bucket '{BUCKET_NAME}' already existance")

# # === UPLOAD EACH ROW AS PARQUET ===
# for _, row in df.iterrows():
#     ticker = row['symbol']
#     year = row.get('calendarYear', None)

#     if pd.isna(year):
#         print(f"⚠️ Row without 'calendarYear', skip: {row}")
#         continue

#     fname = f"{year}.parquet"
#     object_name = f"{ticker}/{fname}"

#     # Converti la riga in DataFrame -> Parquet in memory
#     single_row_df = pd.DataFrame([row])
#     table = pa.Table.from_pandas(single_row_df)
#     buffer = io.BytesIO()
#     pq.write_table(table, buffer)
#     buffer.seek(0)

#     # Upload
#     try:
#         client.put_object(
#             BUCKET_NAME,
#             object_name,
#             data=buffer,
#             length=buffer.getbuffer().nbytes,
#             content_type="application/octet-stream"
#         )
#         print(f"✓ Caricato: {object_name}")
#     except S3Error as e:
#         print(f"❌ Errore su {object_name}: {e}")

# print("Finish!!!")



#!/usr/bin/env python3
import os
import json
import time
import pandas as pd
from kafka import KafkaProducer

# === CONFIG ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "h_company")
PARQUET_FILE = "/app/kp_historical_company/df_company_fundamentals.parquet"

# === CONNESSIONE KAFKA ===
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connesso a Kafka.")
            return producer
        except Exception as e:
            print(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
            time.sleep(5)

producer = connect_kafka()

# === READ PARQUET ===
df = pd.read_parquet(PARQUET_FILE)
print(f"📦 Caricati {len(df)} record da {PARQUET_FILE}")

# === PRODUCE TO KAFKA ===
for _, row in df.iterrows():
    record = row.dropna().to_dict()
    producer.send(KAFKA_TOPIC, value=record)
    print(f"➡️ Inviato {record.get('symbol')} - {record.get('calendarYear')}")

producer.flush()
producer.close()
print("✅ Completato l'invio a Kafka.")
