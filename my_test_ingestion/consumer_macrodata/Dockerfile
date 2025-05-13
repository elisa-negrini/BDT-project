# consumer macrodata/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    kafka-python \
    pandas \
    pyarrow \
    s3fs \
    boto3

COPY consumer_macrodata.py /app/consumer_macrodata.py

CMD ["python", "consumer_macrodata.py"]