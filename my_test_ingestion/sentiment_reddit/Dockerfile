FROM bitnami/spark:latest

USER root

# Installa Python e pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Crea la cartella dell'app
WORKDIR /app

# Copia script e requirements
COPY sentiment_reddit.py /app/sentiment_reddit.py
COPY requirements.txt /app/requirements.txt

# Installa librerie Python
RUN pip3 install -r requirements.txt

# Comando di default
CMD ["python3", "sentiment_reddit.py"]