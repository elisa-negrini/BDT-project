import json
from confluent_kafka import Consumer, Producer, KafkaError
from datetime import datetime, timedelta

# Configurazione Kafka
conf_consumer = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'predictor_group',
    'auto.offset.reset': 'earliest'
}

conf_producer = {
    'bootstrap.servers': 'kafka:9092'
}

consumer = Consumer(conf_consumer)
producer = Producer(conf_producer)

consumer.subscribe(['aggregated_data'])

print("✅ Predictor in ascolto su 'aggregated_data'...")

def compute_target_timestamp(iso_ts):
    try:
        dt = datetime.fromisoformat(iso_ts)
        return (dt + timedelta(minutes=1)).isoformat()
    except ValueError:
        return None

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
            data = json.loads(msg.value().decode('utf-8'))
            ticker = data.get("ticker", "unknown")
            timestamp = data.get("timestamp")
            price = data.get("price_mean_1min")

            if timestamp and price is not None:
                target_timestamp = compute_target_timestamp(timestamp)
                if not target_timestamp:
                    print(f"⚠️ Timestamp non valido: {timestamp}")
                    continue

                prediction_data = {
                    "ticker": ticker,
                    "target_timestamp": target_timestamp,
                    "predicted_price": price
                }

                producer.produce('prediction', json.dumps(prediction_data).encode('utf-8'))
                producer.flush()

                print(f"📤 Predizione inviata: {prediction_data}")
            else:
                print("⚠️ Dati mancanti nel messaggio.")

        except json.JSONDecodeError:
            print("⚠️ Messaggio non decodificabile come JSON.")

except KeyboardInterrupt:
    print("🛑 Interrotto da tastiera.")

finally:
    consumer.close()
