# Dockerfile
FROM python:3.9-slim

# Set workdir
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python \
    transformers \
    scipy

RUN pip install --no-cache-dir --timeout 10000 --retries 10 torch

# Copy source code
COPY sentiment_bluesky.py /app/sentiment_bluesky.py

# Run script
CMD ["python", "sentiment_bluesky.py"]
