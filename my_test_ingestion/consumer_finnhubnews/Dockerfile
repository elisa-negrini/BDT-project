# consumer finnhubnews/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    kafka-python \
    pandas \
    pyarrow \
    s3fs \
    boto3

COPY consumer_finnhubnews.py /app/consumer_finnhubnews.py

CMD ["python", "consumer_finnhubnews.py"]