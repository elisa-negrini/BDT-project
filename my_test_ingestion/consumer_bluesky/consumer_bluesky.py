import time
import json
from kafka import KafkaConsumer

# Parametri da configurare
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'bluesky'
OUTPUT_FILE = 'bluesky.jsonl'

def connect_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='json_saver_group'
            )
            print("✅ Connessione a Kafka riuscita (consumer).")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka non disponibile (consumer), ritento tra 5 secondi... ({e})")
            time.sleep(5)

consumer = connect_kafka_consumer()
print(f"📡 In ascolto sul topic '{TOPIC_NAME}'...")

# Apertura del file in modalità append
with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
    for message in consumer:
        data = message.value  # già deserializzato come dict
        json.dump(data, f, ensure_ascii=False)
        f.write('\n')
        print("💾 Dato salvato:", data)

