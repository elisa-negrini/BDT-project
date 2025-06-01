# FROM bitnami/spark:latest

# USER root

# # Installa Python e pip
# RUN apt-get update && apt-get install -y python3 python3-pip

# # Crea la cartella dell'app
# WORKDIR /app

# # Copia script e requirements
# COPY sentiment_reddit.py /app/sentiment_reddit.py
# COPY requirements.txt /app/requirements.txt

# # Installa librerie Python
# RUN pip3 install -r requirements.txt

# # Comando di default
# CMD ["python3", "sentiment_reddit.py"]


FROM flink:1.17-scala_2.12

# Installa dipendenze di sistema e Python
RUN apt-get update && apt-get install -y python3 python3-pip -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Installa le dipendenze Python necessarie
# (Assicurati che queste siano specifiche per OGNI servizio o comuni a tutti)
RUN pip3 install \
    apache-flink==1.17.0 \
    numpy \
    transformers \
    onnxruntime \
    scipy

# Copia i JAR dei connettori dalla cartella comune alla directory lib di Flink
# NOTA: il percorso 'common_jars/' è relativo alla **nuova build context** che definiremo in docker-compose.yml
COPY jar_flink/*.jar /opt/flink/lib/

# Imposta la directory di lavoro nel container
WORKDIR /app

# CMD per avviare il job Flink Python
# Lo script specifico di ogni servizio (es. sentiment_bluesky.py) verrà montato a /app/
CMD ["python", "/app/sentiment_reddit.py"]