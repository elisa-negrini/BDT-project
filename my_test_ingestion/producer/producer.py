# producer/producer.py

from kafka import KafkaProducer
import time
import json

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate sending data every second
topic = "test_topic"
count = 0
while True:
    message = {"message": f"Hello from producer! {count}"}
    producer.send(topic, message)
    print(f"Sent: {message}")
    time.sleep(1)
    count += 1
