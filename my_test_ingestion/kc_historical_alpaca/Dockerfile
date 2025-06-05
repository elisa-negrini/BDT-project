# kc historical alpaca / Dockerfile

FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY kc_historical_alpaca.py .

CMD ["python", "kc_historical_alpaca.py"]