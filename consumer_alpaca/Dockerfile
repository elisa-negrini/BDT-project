# consumer alpaca/Dockerfile

FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip3 install --no-cache-dir -r requirements.txt

COPY consumer_alpaca.py /app/consumer_alpaca.py

CMD ["python", "consumer_alpaca.py"]
