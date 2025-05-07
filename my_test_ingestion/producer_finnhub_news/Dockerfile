# producer finnhubnews/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    pyspark \
    pandas \
    kafka-python \
    requests

COPY producer_finnhubnews.py /app/producer_finnhubnews.py

CMD ["python", "producer_finnhubnews.py"]
