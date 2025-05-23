#!/usr/bin/env python3
# predict.py

import os
import json
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from neuralprophet import NeuralProphet
from io import StringIO

def main():
    # Load the trained model
    model = NeuralProphet.load("model/neural_model.np")

    # Set up Kafka consumer and producer
    consumer = KafkaConsumer(
        'aggregated_data',
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='neuralprophet-predictor'
    )

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for message in consumer:
        data = message.value

        # Convert single data point into DataFrame
        df = pd.DataFrame([data])
        df = df.rename(columns={"timestamp": "ds"})

        # Run prediction
        forecast = model.predict(df)
        result = forecast.to_dict(orient="records")[-1]  # latest forecast

        # Produce prediction to Kafka topic
        producer.send("aggregated_forecast", result)
        print(f"Published forecast: {result}")

if __name__ == "__main__":
    main()
