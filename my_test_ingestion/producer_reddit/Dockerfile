# producer reddit/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    pyspark \
    pandas \
    kafka-python \
    requests

COPY producer_reddit.py /app/producer_reddit.py

CMD ["python", "producer_reddit.py"]
