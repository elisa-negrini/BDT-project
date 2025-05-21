# Usa un'immagine base Python
FROM python:3.10-slim

# Imposta la directory di lavoro
WORKDIR /app

# Copia i file nello container
COPY prediction_layer2.py /app

# Installa le dipendenze
RUN pip install --no-cache-dir confluent-kafka

# Comando di default per eseguire lo script
CMD ["python", "prediction_layer2.py"]
