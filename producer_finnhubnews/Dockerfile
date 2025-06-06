# producer finnhubnews/Dockerfile

FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY producer_finnhubnews.py /app/producer_finnhubnews.py

CMD ["python", "producer_finnhubnews.py"]
