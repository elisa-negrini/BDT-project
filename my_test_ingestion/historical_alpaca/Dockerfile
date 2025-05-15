# historical alpaca/Dockerfile

FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

RUN apt-get update && apt-get install -y python3-pip && \
    pip install --no-cache-dir \
        alpaca-py \
        pandas

COPY historical_alpaca.py /app/historical_alpaca.py

CMD ["spark-submit", "historical_alpaca.py"]

