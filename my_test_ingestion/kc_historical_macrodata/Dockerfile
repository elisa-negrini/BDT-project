FROM bitnami/spark:latest

USER root
RUN pip install --no-cache-dir alpaca-py boto3 pandas kafka-python

COPY . /app
WORKDIR /app

CMD ["spark-submit", "--master", "spark://spark-master:7077", "kc_historical_macrodata.py"]