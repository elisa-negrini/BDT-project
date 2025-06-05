# kc historical macrodata / Dockerfile

FROM bitnami/spark:latest

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["spark-submit", "--master", "spark://spark-master:7077", "kc_historical_macrodata.py"]