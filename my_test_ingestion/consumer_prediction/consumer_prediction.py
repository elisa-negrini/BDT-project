import os
import json
import time
import pandas as pd
import s3fs
import boto3
from confluent_kafka import Consumer, KafkaError
from botocore.exceptions import ClientError
import pyarrow

# === Configurazione ===
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'prediction'
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'admin')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'admin123')
S3_BUCKET = os.getenv('S3_BUCKET', 'prediction')

# === Inizializza connessione S3 ===
def ensure_bucket_exists():
    s3 = boto3.resource(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    try:
        s3.meta.client.head_bucket(Bucket=S3_BUCKET)
    except ClientError:
        print(f"⚠️ Bucket '{S3_BUCKET}' non trovato, lo creo...")
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"✅ Bucket '{S3_BUCKET}' creato.")

# === File system S3 ===
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT}
)

# === Configura consumer Kafka ===
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'prediction_consumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# === Avvio ===
ensure_bucket_exists()
print(f"✅ Listening to topics: {', '.join([TOPIC])}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"❌ Errore: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print(f"⚠️ Errore parsing JSON: {e}")
            continue

        timestamp = data.get("timestamp")
        ticker = data.get("ticker")
        prediction = data.get("prediction")
        is_simulated_prediction = data.get("is_simulated_prediction")

        if not timestamp or not prediction or not ticker:
            print("⚠️ Messaggio incompleto, saltato. Dump:")
            print(json.dumps(data, indent=2))
            continue


        try:
            ts = pd.to_datetime(timestamp, utc=True)
        except Exception as e:
            print(f"⚠️ Errore parsing timestamp: {e}, saltato.")
            continue


        row = {
                "Ticker": ticker,
                "Timestamp": ts.isoformat(),
                "Prediction": prediction,
                "is_simulated_prediction" : is_simulated_prediction
                }
        
        df = pd.DataFrame([row])

        month_str = f"{ts.year % 100:02d}-{ts.month:02d}"  # Es: '25-01'
        filename = f"{ticker}_{ts.strftime('%Y%m%dT%H%M%S')}.parquet"
        path = f"{S3_BUCKET}/ticker={ticker}/month={month_str}/{filename}"

        try:
            df.to_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs, index=False)
            print(f"✓ Salvato: {path}")
        except Exception as e:
            print(f"❌ Errore salvataggio: {e}")

except KeyboardInterrupt:
    print("🛑 Interrotto da tastiera")
finally:
    consumer.close()