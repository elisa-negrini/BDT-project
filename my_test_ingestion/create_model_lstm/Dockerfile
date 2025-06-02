FROM python:3.10-slim

# Evita input interattivi durante l'installazione
ENV DEBIAN_FRONTEND=noninteractive

# Sopprime log di INFO/WARNING da TensorFlow
ENV TF_CPP_MIN_LOG_LEVEL=2

# Imposta directory di lavoro
WORKDIR /app

# Installa dipendenze di sistema richieste da psycopg2/tensorflow
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia solo requirements e installa prima (per caching efficiente)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copia il resto dei file del progetto
COPY . .

# Esegui lo script
CMD ["python", "train_lstm.py"]
