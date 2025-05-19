import pandas as pd
import os
import io
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
import time

# === CONFIG ===
MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
BUCKET_NAME = "company-fundamentals"

# === CONNECT TO MINIO ===
client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# === ATTENDI MinIO (loop infinito finché non parte) ===
while True:
    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"✓ Creato bucket '{BUCKET_NAME}'")
        else:
            print(f"✓ Bucket '{BUCKET_NAME}' già esistente")
        break
    except Exception as e:
        print(f"⏳ In attesa che MinIO sia pronto... {e}")
        time.sleep(5)


# === READ DATAFRAME ===
file_path = "/app/historical_company/df_company_fundamentals.parquet"
df = pd.read_parquet(file_path)

# === CREATE BUCKET IF NOT EXISTS ===
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
    print(f"✓ Create bucket '{BUCKET_NAME}'")
else:
    print(f"✓ Bucket '{BUCKET_NAME}' already existance")

# === UPLOAD EACH ROW AS PARQUET ===
for _, row in df.iterrows():
    ticker = row['symbol']
    year = row.get('calendarYear', None)

    if pd.isna(year):
        print(f"⚠️ Row without 'calendarYear', skip: {row}")
        continue

    fname = f"{year}.parquet"
    object_name = f"{ticker}/{fname}"

    # Converti la riga in DataFrame -> Parquet in memory
    single_row_df = pd.DataFrame([row])
    table = pa.Table.from_pandas(single_row_df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload
    try:
        client.put_object(
            BUCKET_NAME,
            object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print(f"✓ Caricato: {object_name}")
    except S3Error as e:
        print(f"❌ Errore su {object_name}: {e}")

print("Finish!!!")