# historical alpaca / Dockerfile

FROM python:3.9-slim

USER root

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app
WORKDIR /app

CMD ["python", "kp_historical_alpaca.py"]