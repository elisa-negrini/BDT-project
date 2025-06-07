FROM flink:1.17-scala_2.12

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    build-essential \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY sentiment_analysis/sentiment_bluesky/requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY jar_flink/*.jar /opt/flink/lib/

COPY sentiment_analysis/sentiment_bluesky /app

CMD ["python", "/app/sentiment_bluesky.py"]