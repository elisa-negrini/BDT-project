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

consumer.subscribe(['stock_trades'])

print("✅ Predictor in ascolto su 'stock_trades'...")

def increment_minute_iso8601(timestamp_str):
    try:
        dt = datetime.fromisoformat(timestamp_str)
        dt_next = dt + timedelta(minutes=1)
        return dt_next.isoformat()
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
            price = data.get("price")

            if timestamp and price is not None:
                predicted_price = price  # modello banalissimo
                predicted_timestamp = increment_minute_iso8601(timestamp)

                if predicted_timestamp:
                    prediction_data = {
                        "ticker": ticker,
                        "target-timestamp": predicted_timestamp,
                        "price": predicted_price
                    }

                    producer.produce('prediction', json.dumps(prediction_data).encode('utf-8'))
                    producer.flush()
                    print(f"📤 Prediction inviata: {prediction_data}")
                else:
                    print(f"⚠️ Timestamp non valido: {timestamp}")
            else:
                print("⚠️ Messaggio incompleto (timestamp o price mancante).")

        except json.JSONDecodeError:
            print("⚠️ Messaggio non decodificabile come JSON.")

except KeyboardInterrupt:
    print("🛑 Interrotto da tastiera.")

finally:
    consumer.close()
