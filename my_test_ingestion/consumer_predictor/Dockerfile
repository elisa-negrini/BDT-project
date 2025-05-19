# Dockerfile

FROM python:3.10-slim

WORKDIR /app

# Install required Python packages directly
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    river==0.20.0 \
    joblib==1.4.2

# Copy the prediction script
COPY predictor.py .

# Copy the models into the correct path
COPY ml_modeling/models_multivariate/ ./ml_modeling/models_multivariate/

# Set default Kafka broker (can be overridden in docker-compose)
ENV KAFKA_BROKER=broker:9092

# Run the script
CMD ["python", "predictor.py"]
