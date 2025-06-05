# producer macrodata/Dockerfile

FROM python:3.10-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    pyspark \
    pandas \
    kafka-python \
    requests

COPY producer_macrodata.py /app/producer_macrodata.py

CMD ["python", "producer_macrodata.py"]
