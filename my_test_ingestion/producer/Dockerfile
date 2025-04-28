# producer/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip install kafka-python

COPY producer.py /app/producer.py

CMD ["python", "producer.py"]
