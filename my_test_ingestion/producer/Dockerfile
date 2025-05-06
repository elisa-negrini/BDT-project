# producer/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    pyspark \
    pandas \
    kafka-python \
    alpaca-trade-api \
    alpaca-py \
    websockets \ 
    nest_asyncio


COPY producer.py /app/producer.py

CMD ["python", "producer.py"]
