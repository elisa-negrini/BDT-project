# Script 2: stream_predict_update.py

import json
import joblib
import os
from kafka import KafkaConsumer
from river import compose, linear_model, preprocessing

# CONFIG
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "aggregated_data"
MODEL_DIR = "models_multivariate"

# Pre-load all models into memory
models = {}
for filename in os.listdir(MODEL_DIR):
    if filename.endswith(".model"):
        ticker = filename.replace(".model", "")
        models[ticker] = joblib.load(os.path.join(MODEL_DIR, filename))

print(f"✅ Loaded {len(models)} models.")

# Start Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="prediction_group"
)

print(f"🔁 Listening to topic '{TOPIC}' on {KAFKA_BROKER}...")

for message in consumer:
    data = message.value
    ticker = data.get("ticker")

    if ticker not in models:
        print(f"⚠️  No model for ticker {ticker}, skipping.")
        continue

    model = models[ticker]

    # Remove 'ticker' from input; make sure all required features are there
    x = {k: v for k, v in data.items() if k != "ticker"}

    if any(v is None for v in x.values()):
        print(f"⚠️  Missing values in data for {ticker}, skipping.")
        continue

    try:
        prediction = model.predict_one(x)
        print(f"[{ticker}] ➜ Predicted price_mean_1min in 5 min: {prediction:.4f}")
    except Exception as e:
        print(f"❌ Error predicting for {ticker}: {e}")

