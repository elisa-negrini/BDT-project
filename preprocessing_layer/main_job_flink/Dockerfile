FROM flink:1.17-scala_2.12

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY preprocessing_layer/main_job_flink/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY preprocessing_layer/main_job_flink/main_job.py .

COPY jar_flink/*.jar /opt/flink/lib/

CMD ["python", "/app/main_job.py"]