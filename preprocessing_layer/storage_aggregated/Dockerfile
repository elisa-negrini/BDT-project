# FROM flink:1.17-scala_2.12

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     python3 \
#     python3-pip \
#     && rm -rf /var/lib/apt/lists/*

# RUN ln -s /usr/bin/python3 /usr/bin/python

# WORKDIR /app

# COPY preprocessing_stream/storage_aggregated/requirements.txt .

# RUN pip install --no-cache-dir -r requirements.txt

# COPY preprocessing_stream/storage_aggregated/storage_aggregated.py .

# #COPY jar_flink/*.jar /opt/flink/lib/
# COPY preprocessing_stream/storage_aggregated/flink-connector-kafka-1.17.0.jar /opt/flink/lib/
# COPY preprocessing_stream/storage_aggregated/kafka-clients-3.3.2.jar /opt/flink/lib/

# CMD ["/opt/flink/bin/flink", "run", "-py", "/app/storage_aggregated.py"]

FROM flink:1.17.2-scala_2.12

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY preprocessing_layer/storage_aggregated/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY preprocessing_layer/storage_aggregated/storage_aggregated.py .

COPY jar_flink/flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY jar_flink/kafka-clients-3.3.2.jar /opt/flink/lib/

CMD ["bash", "-c", "/opt/flink/bin/start-cluster.sh && \
    /opt/flink/bin/flink run -py /app/storage_aggregated.py && \
    /opt/flink/bin/stop-cluster.sh"]