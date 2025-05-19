# historical alpaca/Dockerfile

FROM bitnami/spark:latest

USER root
RUN pip install --no-cache-dir alpaca-py boto3 pandas

COPY . /app
WORKDIR /app

CMD ["spark-submit", "--master", "spark://spark-master:7077", "historical_alpaca.py"]