# FROM flink:1.17-scala_2.12

# RUN apt-get update && apt-get install -y python3 python3-pip
# RUN ln -s /usr/bin/python3 /usr/bin/python

# RUN pip3 install apache-beam

# RUN pip3 install apache-flink==1.17.0 numpy boto3 kafka-python python-dateutil pytz pyarrow minio pandas

# # Copia script Python e template
# COPY main_job.py /opt/main_job.py

# # Copia i JAR per Kafka
# COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
# COPY kafka-clients-3.3.2.jar /opt/flink/lib/


# CMD ["python", "/opt/main_job.py"]


FROM flink:1.17-scala_2.12

# 1. Aggiorna i pacchetti di sistema e installa Python/pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 2. Crea il link simbolico per python (anche se pip3 è preferibile)
RUN ln -s /usr/bin/python3 /usr/bin/python

# 3. Installa PyFlink e le sue dipendenze PRIMA di altre librerie.
#    apache-flink installa già le dipendenze Beam compatibili.
#    Usa --no-cache-dir per evitare problemi di cache di pip.
RUN pip3 install --no-cache-dir apache-flink==1.17.0

# 4. Installa le altre dipendenze Python necessarie
RUN pip3 install --no-cache-dir \
    numpy \
    boto3 \
    kafka-python \
    python-dateutil \
    pytz \
    pyarrow \
    minio \
    pandas

# Copia script Python e template
COPY main_job.py /opt/main_job.py

# Copia i JAR per Kafka nella directory lib di Flink
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/

# Esegui lo script Python
CMD ["python", "/opt/main_job.py"]