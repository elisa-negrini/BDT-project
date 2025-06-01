# FROM bitnami/spark:latest

# USER root

# # Installa Python e pip
# RUN apt-get update && apt-get install -y python3 python3-pip wget

# # Crea la cartella dell'app
# WORKDIR /app

# # Copia lo script e il requirements.txt
# COPY . /app/


# # Installa le librerie Python dal requirements.txt
# RUN pip3 install --no-cache-dir -r /app/requirements.txt

# # Comando di default
# CMD ["python3", "sentiment_bluesky.py"]

FROM flink:1.17-scala_2.12

# Installa dipendenze di sistema e Python
RUN apt-get update && apt-get install -y python3 python3-pip -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Installa le dipendenze Python necessarie per il tuo script Flink
RUN pip3 install \
    apache-flink==1.17.0 \
    numpy \
    transformers \
    onnxruntime \
    scipy # Aggiungi scipy per softmax

# Copia i JAR dei connettori specifici nella directory lib di Flink
# Questi JAR si trovano ora in ./flink_connectors/ all'interno del contesto di build
COPY flink_connectors/*.jar /opt/flink/lib/

# CMD per avviare il job Flink Python
# Il tuo script sentiment_bluesky.py sarà montato in /app/
CMD ["python", "/app/sentiment_bluesky.py"]