FROM bitnami/spark:latest

USER root

# Installa Python e pip
RUN apt-get update && apt-get install -y python3 python3-pip
# Crea la cartella dell'app
WORKDIR /app

# Copia lo script
COPY sentiment_reddit.py /app/sentiment_reddit.py

# Installa librerie Python
RUN pip3 install --no-cache-dir torch transformers scipy

# Comando di default
CMD ["python3", "sentiment_reddit.py"]
