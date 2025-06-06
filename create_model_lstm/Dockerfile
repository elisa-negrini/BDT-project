FROM python:3.10-slim

# Evita input interattivi durante l'installazione
ENV DEBIAN_FRONTEND=noninteractive

# Sopprime log di INFO/WARNING da TensorFlow
ENV TF_CPP_MIN_LOG_LEVEL=2

# Imposta directory di lavoro principale
WORKDIR /app

# Copia solo requirements e installa prima (per caching efficiente)
COPY create_model_lstm/requirements.txt ./requirements.txt
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

# Copia solo la cartella necessaria (più sicuro e mirato)
COPY create_model_lstm /app/create_model_lstm

# Imposta la working dir per lo script
WORKDIR /app/create_model_lstm

# Comando finale (può essere overrideato in docker-compose)
CMD ["python", "train_lstm.py"]

# FROM python:3.10-slim

# # Evita input interattivi durante l'installazione
# ENV DEBIAN_FRONTEND=noninteractive

# # Sopprime log di INFO/WARNING da TensorFlow
# ENV TF_CPP_MIN_LOG_LEVEL=2

# # Imposta directory di lavoro
# WORKDIR /app

# COPY . /app/

# # Installa dipendenze di sistema richieste da psycopg2/tensorflow
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     python3-dev \
#     libpq-dev \
#     && rm -rf /var/lib/apt/lists/*

# # Copia solo requirements e installa prima (per caching efficiente)
# COPY create_model_lstm/requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# WORKDIR /app/create_model_lstm 
# # Copia il resto dei file del progetto
# #COPY . .

# # # Esegui lo script
# # CMD ["python", "train_lstm.py"]
