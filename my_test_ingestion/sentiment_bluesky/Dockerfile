FROM bitnami/spark:latest

USER root

# Installa Python e pip
RUN apt-get update && apt-get install -y python3 python3-pip wget

# Crea la cartella dell'app
WORKDIR /app

# Copia lo script
COPY sentiment_bluesky.py /app/sentiment_bluesky.py 

# Installa librerie Python necessarie
RUN pip3 install torch transformers scipy

# Comando di default
CMD ["python3", "sentiment_bluesky.py"]