from minio import Minio
from minio.deleteobjects import DeleteObject
from minio.error import S3Error
import os

# === CONFIG ===
MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
BUCKET_NAMES = os.getenv(
    "BUCKET_NAME",
    "bluesky-data,bluesky-sentiment,finnhub-data,historical-data,macro-data,reddit-data,reddit-sentiment,stock-data"
).split(",")

# === CONNECT ===   
client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# === DELETE OBJECTS IN EACH BUCKET ===
for bucket_name in BUCKET_NAMES:
    if client.bucket_exists(bucket_name):
        print(f"🧹 Pulizia bucket '{bucket_name}'...")
        objects = client.list_objects(bucket_name, recursive=True)
        delete_objects = [DeleteObject(obj.object_name) for obj in objects]

        if delete_objects:
            for del_err in client.remove_objects(bucket_name, delete_objects):
                print(f"❌ Errore rimozione: {del_err}")
            print(f"✅ Tutti i file eliminati da '{bucket_name}'.")
        else:
            print(f"ℹ️ Il bucket '{bucket_name}' è già vuoto.")
    else:
        print(f"❌ Il bucket '{bucket_name}' non esiste.")