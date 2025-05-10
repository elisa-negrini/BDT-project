from kafka import KafkaConsumer, KafkaProducer
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import torch
import json

# Kafka Config
KAFKA_BROKER = 'kafka:9092'
SOURCE_TOPIC = 'bluesky'
TARGET_TOPIC = 'bluesky_sentiment'

# FinBERT Model
MODEL_NAME = "ProsusAI/finbert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

# Kafka Consumer
consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='finbert-consumer-group'
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_finbert_sentiment(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = softmax(logits.numpy()[0])
    return {
        "positive_prob": float(probs[0]),
        "neutral_prob": float(probs[1]),
        "negative_prob": float(probs[2])
    }

print("📡 In ascolto di nuovi post per analisi FinBERT...")

for message in consumer:
    post = message.value
    text = post.get("text", "")

    if not text.strip():
        continue

    sentiment = get_finbert_sentiment(text)
    post.update(sentiment)

    print(f"🔍 Analizzato post: {post['id']} | Pos: {sentiment['positive_prob']:.2f}, Neg: {sentiment['negative_prob']:.2f}")
    #producer.send(TARGET_TOPIC, value=post)
