from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'kafka:9092',  # Usa il nome del servizio Kafka nel Docker network
    'group.id': 'sentiment_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topics = ['reddit_sentiment', 'news_sentiment', 'bluesky_sentiment']
consumer.subscribe(topics)

print(f"✅ Listening to topics: {', '.join(topics)}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"❌ Errore: {msg.error()}")
            continue

        print(f"📥 Topic: {msg.topic()} | Key: {msg.key()} | Value: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("🛑 Interrotto da tastiera")
finally:
    consumer.close()
