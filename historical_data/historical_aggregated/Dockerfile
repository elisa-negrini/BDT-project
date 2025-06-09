FROM flink:1.17-scala_2.12

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    libpq-dev \
    gcc && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY historical_data/historical_aggregated/requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY jar_flink/*.jar /opt/flink/lib/

COPY historical_data/historical_aggregated/historical_aggregated.py /app/historical_aggregated.py

CMD ["python", "historical_aggregated.py"]