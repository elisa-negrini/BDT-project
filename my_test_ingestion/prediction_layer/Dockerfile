FROM python:3.10-slim

WORKDIR /app

COPY prediction_layer.py .

RUN pip install confluent_kafka

CMD ["python", "prediction_layer.py"]
