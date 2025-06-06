# kp historical company / Dockerfile

FROM python:3.9-slim

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY df_company_fundamentals.parquet ./kp_historical_company/

COPY kp_historical_company.py .

CMD ["python", "kp_historical_company.py"]