# FROM python:3.9-slim-buster

# # Imposta la directory di lavoro all'interno del container
# WORKDIR /app

# # Copia il file requirements.txt e installa le dipendenze
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # Crea la directory 'model' all'interno di /app
# RUN mkdir -p model 

# # Copia gli artefatti del modello nella directory 'model' all'interno del container
# # Il percorso di origine (il primo argomento di COPY) è relativo al CONTESTO DI BUILD
# COPY create_model_lstm/ create_model_lstm/

# # Copia lo script dell'applicazione dalla sua posizione relativa al contesto di build
# COPY prediction_layer.py /app

# # Definisci le variabili d'ambiente (opzionale, ma utile per configurare Kafka)
# ENV KAFKA_BROKER=localhost:9092
# ENV KAFKA_TOPIC=aggregated_data

# # Comando per avviare l'applicazione quando il container viene eseguito
# CMD ["python", "prediction_layer.py"]



FROM flink:1.17-scala_2.12

# Imposta la directory di lavoro nel container
WORKDIR /app

# Installa Python e pip
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Copia e installa i requirements
COPY ./prediction_layer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia artefatti del modello
RUN mkdir -p create_model_lstm/model
COPY create_model_lstm/model/ /opt/flink/model/

# Copia i jar necessari in Flink
COPY jar_flink/*.jar /opt/flink/lib/

# Copia lo script PyFlink
COPY ./prediction_layer/prediction_layer.py .

# Variabili d'ambiente
ENV KAFKA_BROKER=kafka:9092
ENV KAFKA_TOPIC=aggregated_data

# Comando per eseguire lo script di predizione
CMD ["python3", "prediction_layer.py"]




# FROM bitnami/spark:latest

# USER root

# # Installa Python e pip
# RUN apt-get update && apt-get install -y python3 python3-pip wget

# # Crea la cartella dell'app
# WORKDIR /app

# # Copia lo script e il requirements.txt
# COPY ./prediction_layer /app/


# # Installa le librerie Python dal requirements.txt
# RUN pip3 install --no-cache-dir -r /app/requirements.txt

# # Comando di default
# CMD ["python3", "prediction_layer.py"]