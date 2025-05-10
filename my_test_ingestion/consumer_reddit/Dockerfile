# consumer reddit/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    kafka-python \
    pandas \
    pyarrow \
    s3fs 

COPY consumer_reddit.py /app/consumer_reddit.py

CMD ["python", "consumer_reddit.py"]