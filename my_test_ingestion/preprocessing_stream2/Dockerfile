# Usa un'immagine base Python
FROM python:3.10-slim-buster 

# Imposta la directory di lavoro
WORKDIR /app

# Installa le dipendenze direttamente
RUN pip install minio pandas pyarrow --no-cache-dir --break-system-packages # <--- MODIFICATO QUI (rimosso -r)

# Copia lo script di test
COPY read_fundamentals.py .

# Comando per eseguire lo script quando il container parte
CMD ["python", "read_fundamentals.py"]