FROM bitnami/spark:latest

USER root

# Crea la cartella dell'app
WORKDIR /app

# Copia lo script
COPY sentiment_news.py /app/sentiment_news.py

# Installa librerie Python
RUN pip3 install torch transformers scipy

# Comando di default
CMD ["python3", "sentiment_news.py"]
