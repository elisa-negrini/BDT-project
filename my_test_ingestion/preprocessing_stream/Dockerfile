FROM flink:1.17-scala_2.12

RUN apt-get update && apt-get install -y python3 python3-pip
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-beam==2.46.0

RUN pip3 install apache-flink==1.17.0 numpy boto3 kafka-python python-dateutil pytz pyarrow minio pandas

# Copia script Python e template
COPY preprocess_flink.py /opt/preprocess_flink.py

# Copia i JAR per Kafka
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/
#COPY flink-parquet-1.17.0.jar /opt/flink/lib/
#COPY hadoop-common-3.3.6.jar /opt/flink/lib/

# --- Connettori MinIO (S3A) ---
#COPY hadoop-aws-3.3.6.jar /opt/flink/lib/
#COPY aws-java-sdk-bundle-1.11.1026.jar /opt/flink/lib/

CMD ["python", "/opt/preprocess_flink.py"]
#CMD ["flink", "run", "-py", "/opt/preprocess_flink.py"]

