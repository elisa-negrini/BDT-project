# consumer bluesky/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    kafka-python \
    pandas \
    pyarrow \
    s3fs \
    boto3

COPY consumer_bluesky.py /app/consumer_blueksy.py

CMD ["python", "consumer_bluesky.py"]