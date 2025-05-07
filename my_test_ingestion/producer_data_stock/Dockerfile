# producer Data Stream/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    pyspark \
    pandas \
    kafka-python \
    websockets \ 
    nest_asyncio


COPY producer_data_stock.py /app/producer_data_stock.py

CMD ["python", "producer_data_stock.py"]
