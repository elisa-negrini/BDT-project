FROM apache/flink:1.17-scala_2.12-java11

USER root

RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.17 numpy pandas holidays


# Copia il connettore Kafka nella lib di Flink
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.1.jar /opt/flink/lib/


# Copia configurazione e job
#COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml
WORKDIR /opt/flink/jobs
COPY preprocess_flink.py .

# 🔁 Avvia PyFlink come script standalone
CMD ["python3", "/opt/flink/jobs/preprocess_flink.py"]
# questo funzionava e dava i warnins!
# # Copia la configurazione custom di Flink (se presente)
# COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
# COPY kafka-clients-2.4.1.jar /opt/flink/lib/

# WORKDIR /opt/flink/jobs

# COPY preprocess_flink.py .

# CMD ["flink", "run", "-py", "/opt/flink/jobs/preprocess_flink.py"]



# FROM flink:1.17.1-scala_2.12-java11

# USER root
# WORKDIR /opt/flink/jobs

# # Installa Python e pip (per PyFlink)
# RUN apt-get update && apt-get install -y python3 python3-pip && \
#     pip3 install apache-flink==1.17

# # Copia lo script
# COPY preprocess_flink.py .

# # Esegui PyFlink
# CMD ["python3", "/opt/flink/jobs/preprocess_flink.py"]
