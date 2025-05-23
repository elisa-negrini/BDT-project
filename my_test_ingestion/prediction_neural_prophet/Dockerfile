# Base image
FROM python:3.11-slim

# Install dependencies
RUN pip install --no-cache-dir neuralprophet kafka-python pandas

# Create app directory
WORKDIR /app

# Copy script and model directory
COPY predict.py /app/
COPY model /app/model/

# Set environment variables for Kafka (can be overridden)
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Command to run the prediction service
CMD ["python", "predict.py"]