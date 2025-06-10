FROM flink:1.17-scala_2.12

RUN apt-get update && apt-get install -y python3 python3-pip -y --no-install-recommends && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install \
    apache-flink==1.17.0 \
    numpy \
    transformers \
    onnxruntime \
    scipy

COPY jar_flink/*.jar /opt/flink/lib/

WORKDIR /app

CMD ["python", "/app/sentiment_reddit.py"]