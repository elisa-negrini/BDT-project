# kc historical company / Dockerfile

FROM python:3.9-slim

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "kc_historical_company.py"]