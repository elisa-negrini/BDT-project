FROM bitnami/spark:latest

USER root

RUN pip install --no-cache-dir pyspark

WORKDIR /app
COPY spark_writer.py .
