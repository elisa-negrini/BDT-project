# kc historical macrodata / Dockerfile

FROM bitnami/spark:latest

USER root
RUN pip install --no-cache-dir boto3 pandas kafka-python s3fs pyarrow

COPY . /app
WORKDIR /app

CMD ["spark-submit", "--master", "spark://spark-master:7077", "kc_historical_macrodata.py"]