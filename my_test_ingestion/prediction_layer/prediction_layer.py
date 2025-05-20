import json
from confluent_kafka import Consumer, KafkaError

# Configurazione Kafka
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'predictor_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['aggregated_data'])

print("✅ Predictor in ascolto su 'aggregated_data'...")

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
            timestamp = data.get("timestamp", "N/A")
            price = data.get("price_mean_1min", None)

            if price is not None:
                prediction = price  # modello banalissimo
                print(f"📈 Ticker: {ticker} | Time: {timestamp} | Predicted 1-min ahead: {prediction}")
            else:
                print(f"⚠️ Nessun valore 'price_mean_1min' nel messaggio.")

        except json.JSONDecodeError:
            print("⚠️ Messaggio non decodificabile come JSON.")

except KeyboardInterrupt:
    print("🛑 Interrotto da tastiera.")

finally:
    consumer.close()
