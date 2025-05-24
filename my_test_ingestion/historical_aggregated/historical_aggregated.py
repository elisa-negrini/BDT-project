from confluent_kafka import Consumer, KafkaException

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'kafka:9092',  # Change to your Kafka broker address
    'group.id': 'historical-data-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Subscribe to the desired topics
topics = ['historical_alpaca', 'historical_macrodata', 'historical_company']
consumer.subscribe(topics)

print("Waiting for messages from Kafka...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            print(f"Message received from topic '{msg.topic()}': {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    print("Interrupted by user")
finally:
    # Close the consumer
    consumer.close()
