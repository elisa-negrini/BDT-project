# producer bluesky/Dockerfile

FROM python:3.10-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    pyspark \
    pandas \
    kafka-python \
    requests

COPY producer_bluesky.py /app/producer_bluesky.py

CMD ["python", "producer_bluesky.py"]
