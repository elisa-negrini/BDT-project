FROM python:3.10-slim

WORKDIR /app

# Copia file requirements e installa dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia lo script Python
COPY consumer_sentiment.py .

# Comando di avvio
CMD ["python", "consumer_sentiment.py"]
