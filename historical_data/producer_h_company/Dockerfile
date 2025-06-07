# producer_h_company / Dockerfile

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

COPY df_company_fundamentals.parquet ./producer_h_company/

COPY producer_h_company.py .

CMD ["python", "producer_h_company.py"]