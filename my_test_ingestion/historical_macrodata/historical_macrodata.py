# import os
# import json
# import time
# import requests
# from datetime import datetime
# import pandas as pd
# import boto3
# from botocore.exceptions import EndpointConnectionError, ClientError
# import s3fs

# # === Config ===
# API_KEY = "119c5415679f73cb0da3b62e9c2a534d"
# BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
# S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'admin')
# S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'admin123')
# S3_BUCKET = os.getenv('S3_BUCKET', 'macro-data')

# series_dict = {
#     "GDP": "gdp_nominal",
#     "GDPC1": "gdp_real",
#     "A191RL1Q225SBEA": "gdp_qoq",
#     "CPIAUCSL": "cpi",
#     "CPILFESL": "cpi_core",
#     "FEDFUNDS": "ffr",
#     "DGS10": "t10y",
#     "DGS2": "t2y",
#     "T10Y2Y": "spread_10y_2y",
#     "UNRATE": "unemployment",
#     "UMCSENT": "sentiment"
# }

# today = datetime.today().strftime('%Y-%m-%d')

# # === Aspetta MinIO e crea bucket se serve ===
# def wait_for_minio():
#     while True:
#         try:
#             s3 = boto3.client(
#                 's3',
#                 endpoint_url=S3_ENDPOINT,
#                 aws_access_key_id=S3_ACCESS_KEY,
#                 aws_secret_access_key=S3_SECRET_KEY
#             )
#             s3.list_buckets()
#             print("✅ MinIO è pronto.")
#             return
#         except EndpointConnectionError as e:
#             print(f"⏳ MinIO non disponibile, ritento tra 5 sec... ({e})")
#             time.sleep(5)

# def ensure_bucket_exists():
#     s3 = boto3.resource(
#         's3',
#         endpoint_url=S3_ENDPOINT,
#         aws_access_key_id=S3_ACCESS_KEY,
#         aws_secret_access_key=S3_SECRET_KEY
#     )
#     try:
#         s3.meta.client.head_bucket(Bucket=S3_BUCKET)
#         print(f"✅ Bucket '{S3_BUCKET}' esiste.")
#     except ClientError:
#         print(f"📦 Creo bucket '{S3_BUCKET}'...")
#         s3.create_bucket(Bucket=S3_BUCKET)

# # === Salva in Parquet su MinIO ===
# def save_parquet(alias, df):
#     fs = s3fs.S3FileSystem(
#         anon=False,
#         key=S3_ACCESS_KEY,
#         secret=S3_SECRET_KEY,
#         client_kwargs={'endpoint_url': S3_ENDPOINT}
#     )

#     date_str = datetime.today().strftime("%Y-%m-%d")
#     filename = f"{alias}_historical_{date_str}.parquet"
#     path = f"{S3_BUCKET}/{alias}/{filename}"

#     try:
#         df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
#         print(f"✅ Salvato: {path}")
#     except Exception as e:
#         print(f"❌ Errore salvataggio {alias}: {e}")

# # === Fetch storico ===
# def fetch_historical_data():
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
#                     df = pd.DataFrame(records)
#                     save_parquet(alias, df)
#                 else:
#                     print(f"⚠️ Nessun dato valido per {alias}")
#             else:
#                 print(f"❌ Errore FRED per {series_id}: status {response.status_code}")
#         except Exception as e:
#             print(f"❌ Errore richiesta {series_id}: {e}")

# # === Avvio ===
# if __name__ == "__main__":
#     wait_for_minio()
#     ensure_bucket_exists()
#     fetch_historical_data()

import os
import json
import time
import requests
from datetime import datetime
import pandas as pd
import boto3
from botocore.exceptions import EndpointConnectionError, ClientError
import s3fs

# === Config ===
API_KEY = "119c5415679f73cb0da3b62e9c2a534d"
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'admin')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'admin123')
S3_BUCKET = os.getenv('S3_BUCKET', 'macro-data')

series_dict = {
    "GDP": "gdp_nominal",
    "GDPC1": "gdp_real",
    "A191RL1Q225SBEA": "gdp_qoq",
    "CPIAUCSL": "cpi",
    "CPILFESL": "cpi_core",
    "FEDFUNDS": "ffr",
    "DGS10": "t10y",
    "DGS2": "t2y",
    "T10Y2Y": "spread_10y_2y",
    "UNRATE": "unemployment",
    "UMCSENT": "sentiment"
}

# === Attesa MinIO ===
def wait_for_minio():
    while True:
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY
            )
            s3.list_buckets()
            print("✅ MinIO è pronto.")
            return
        except EndpointConnectionError as e:
            print(f"⏳ MinIO non disponibile, ritento tra 5 sec... ({e})")
            time.sleep(5)

# === Crea bucket se non esiste ===
def ensure_bucket_exists():
    s3 = boto3.resource(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    try:
        s3.meta.client.head_bucket(Bucket=S3_BUCKET)
        print(f"✅ Bucket '{S3_BUCKET}' esiste.")
    except ClientError:
        print(f"📦 Creo bucket '{S3_BUCKET}'...")
        s3.create_bucket(Bucket=S3_BUCKET)

# === Salva ogni riga in file Parquet partizionato per anno ===
def save_rows_as_parquet(alias, df):
    fs = s3fs.S3FileSystem(
        anon=False,
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT}
    )

    for _, row in df.iterrows():
        date_str = row['Date']
        year = pd.to_datetime(date_str).year
        value = row['Value']
        single_df = pd.DataFrame([{"Date": date_str, "Value": value}])

        filename = f"{alias}_{date_str}.parquet"
        path = f"{S3_BUCKET}/{alias}/year={year}/{filename}"

        try:
            single_df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
            print(f"✅ Salvato: {path}")
        except Exception as e:
            print(f"❌ Errore salvataggio {alias} {date_str}: {e}")

# === Scarica dati da FRED ===
def fetch_historical_data():
    for series_id, alias in series_dict.items():
        url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json"
        print(f"⬇️  Scarico {alias}...")
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json().get("observations", [])
                records = [
                    {"Date": obs["date"], "Value": float(obs["value"])}
                    for obs in data if obs["value"] != "."
                ]
                if records:
                    df = pd.DataFrame(records)
                    save_rows_as_parquet(alias, df)
                else:
                    print(f"⚠️ Nessun dato valido per {alias}")
            else:
                print(f"❌ Errore FRED per {series_id}: status {response.status_code}")
        except Exception as e:
            print(f"❌ Errore richiesta {series_id}: {e}")

# === Avvio ===
if __name__ == "__main__":
    print("🚀 Avvio fetch storico macroeconomico")
    wait_for_minio()
    ensure_bucket_exists()
    fetch_historical_data()
