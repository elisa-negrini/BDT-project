FROM python:3.9-slim

WORKDIR /app


RUN pip3 install --no-cache-dir \
    kafka-python

COPY consumer.py /app/consumer.py

CMD ["python", "consumer.py"]
