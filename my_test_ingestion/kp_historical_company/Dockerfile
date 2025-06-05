# kp historical company / Dockerfile

FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY df_company_fundamentals.parquet ./kp_historical_company/

COPY kp_historical_company.py .

CMD ["python", "kp_historical_company.py"]