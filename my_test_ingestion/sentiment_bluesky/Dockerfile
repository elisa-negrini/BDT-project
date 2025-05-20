FROM bitnami/spark:latest

USER root

# Installa Python e pip
RUN apt-get update && apt-get install -y python3 python3-pip wget

# Crea la cartella dell'app
WORKDIR /app

# Copia lo script e il requirements.txt
COPY sentiment_bluesky.py /app/sentiment_bluesky.py
COPY requirements.txt /app/requirements.txt

# Installa le librerie Python dal requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Comando di default
CMD ["python3", "sentiment_bluesky.py"]
