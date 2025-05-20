FROM python:3.10-slim

WORKDIR /app

COPY consumer_sentiment.py .

RUN pip install confluent_kafka

CMD ["python", "consumer_sentiment.py"]