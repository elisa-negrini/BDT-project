FROM bitnami/spark:3.5

USER root

# Install Python packages needed per job
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --no-cache-dir requests boto3

USER 1001

# Working dir
WORKDIR /app

# Copia lo script nel container
COPY historical_macrodata_spark.py .

# Default CMD: esegui lo script via spark-submit
CMD ["spark-submit", "--master", "spark://spark-master:7077", "historical_macrodata_spark.py"]
