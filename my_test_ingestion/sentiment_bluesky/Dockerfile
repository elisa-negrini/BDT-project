# Dockerfile
FROM python:3.10-slim

# Set workdir
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python \
    transformers \
    torch \
    scipy

# Copy source code
COPY sentiment_bluesky.py /app/sentiment_bluesky.py

# Run script
CMD ["python", "sentiment_bluesky.py"]
