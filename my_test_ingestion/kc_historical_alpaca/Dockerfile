# kc historical alpaca / Dockerfile

FROM python:3.10-slim

WORKDIR /app


RUN pip install --no-cache-dir kafka-python boto3 pandas pyarrow

# Copia del codice
COPY kc_historical_alpaca.py .

# Entry point
CMD ["python", "kc_historical_alpaca.py"]
