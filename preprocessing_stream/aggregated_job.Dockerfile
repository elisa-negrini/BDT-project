FROM flink:1.17-scala_2.12

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    openjdk-11-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY preprocessing_stream/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY preprocessing_stream/aggregated_job.py .

COPY jar_flink/*.jar /opt/flink/lib/

CMD ["python", "/app/aggregated_job.py"]