from kafka import KafkaConsumer
import json
import os

# Topic corretto usato dal producer
topic = "test_topic"
kafka_server = os.getenv("KAFKA_SERVER", "kafka:9092")

# Percorso dove salvare i dati
output_path = "/data/finnhub_trades.jsonl"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[kafka_server],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"🔌 In ascolto sul topic '{topic}'... Salvando in {output_path}")

# Scrive ogni messaggio in formato JSON line-by-line
with open(output_path, "a", encoding="utf-8") as f:
    for message in consumer:
        data = message.value
        print(f"📝 Ricevuto: {data['Ticker']} @ {data['Price']} ({data['Timestamp']})")
        json.dump(data, f, ensure_ascii=False)
        f.write("\n")
