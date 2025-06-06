# FROM bitnami/spark:latest

# USER root

# # Crea la cartella dell'app
# WORKDIR /app

# # Copia lo script e il requirements.txt
# COPY sentiment_news.py /app/sentiment_news.py
# COPY requirements.txt /app/requirements.txt

# # Installa le librerie Python dal requirements.txt
# RUN pip3 install --no-cache-dir -r requirements.txt

# # Comando di default
# CMD ["python3", "sentiment_news.py"]



FROM flink:1.17-scala_2.12

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    build-essential \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY sentiment_news/requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY jar_flink/*.jar /opt/flink/lib/

COPY . .

CMD ["python", "/app/sentiment_news.py"]