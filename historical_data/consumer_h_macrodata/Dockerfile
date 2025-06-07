# consumer_h_macrodata / Dockerfile

FROM bitnami/spark:latest

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["spark-submit", "--master", "spark://spark-master:7077", "consumer_h_macrodata.py"]