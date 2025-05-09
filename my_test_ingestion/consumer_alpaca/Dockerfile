# consumer alpaca/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip3 install --no-cache-dir \
    kafka-python \
    pandas \
    pyarrow \
    s3fs 

COPY consumer_alpaca.py /app/consumer_alpaca.py

CMD ["python", "consumer_alpaca.py"]