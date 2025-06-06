# historical alpaca / Dockerfile

FROM python:3.9-slim

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

CMD ["python", "kp_historical_alpaca.py"]