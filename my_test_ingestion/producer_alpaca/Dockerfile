# producer alpaca/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    pyspark \
    pandas \
    kafka-python \
    requests \
    alpaca-py \
    pytz \
    nest_asyncio \
    aiohttp

COPY producer_alpaca.py /app/producer_alpaca.py

CMD ["python", "producer_alpaca.py"]