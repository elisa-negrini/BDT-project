FROM bitnami/spark:latest

USER root

# Crea la cartella dell'app
WORKDIR /app

# Copia lo script e il requirements.txt
COPY sentiment_news.py /app/sentiment_news.py
COPY requirements.txt /app/requirements.txt

# Installa le librerie Python dal requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Comando di default
CMD ["python3", "sentiment_news.py"]

