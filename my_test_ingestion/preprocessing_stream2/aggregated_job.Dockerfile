FROM flink:1.17-scala_2.12

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    openjdk-11-jre-headless \ 
    curl \ 
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==1.17.0

RUN pip3 install --no-cache-dir \
    numpy \
    boto3 \
    kafka-python \
    python-dateutil \
    pytz \
    pyarrow \
    minio \
    pandas \
    kafka-python

COPY aggregated_job.py /opt/aggregated_job.py

# JAR
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/

CMD ["python", "/opt/aggregated_job.py"]

