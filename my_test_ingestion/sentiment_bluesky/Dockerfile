FROM bitnami/spark:latest

USER root

# Installa Python e pip
RUN apt-get update && apt-get install -y python3 python3-pip wget

# Crea la cartella dell'app
WORKDIR /app

# Copia lo script
COPY sentiment_bluesky.py /app/sentiment_bluesky.py 

# Installa librerie Python necessarie
RUN pip3 install torch transformers scipy

# # Aggiungi i JAR per Hadoop AWS/S3 support (necessari per MinIO)
# RUN mkdir -p /opt/spark/jars && \
#     wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars && \
#     wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -P /opt/spark/jars

# Comando di default
CMD ["python3", "sentiment_bluesky.py"]
