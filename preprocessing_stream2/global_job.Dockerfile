FROM flink:1.17-scala_2.12

RUN apt-get update && apt-get install -y python3 python3-pip
RUN ln -s /usr/bin/python3 /usr/bin/python

# RUN pip3 install apache-beam==2.46.0

RUN pip3 install apache-flink==1.17.0 numpy boto3 kafka-python python-dateutil pytz pyarrow minio pandas

# Copia script Python e template
COPY global_job.py /opt/global_job.py

# Copia i JAR per Kafka
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/


CMD ["python", "/opt/global_job.py"]
