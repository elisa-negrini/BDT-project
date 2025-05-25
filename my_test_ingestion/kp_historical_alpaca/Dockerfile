# historical alpaca / Dockerfile

FROM python:3.9-slim
USER root
RUN pip install --no-cache-dir kafka-python pandas alpaca-py pyarrow requests

COPY . /app
WORKDIR /app

CMD ["python", "kp_historical_alpaca.py"]