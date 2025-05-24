FROM python:3.9-slim

WORKDIR /app

COPY df_company_fundamentals.parquet ./kp_historical_company/

COPY kp_historical_company.py .

RUN pip install pandas pyarrow minio kafka-python

CMD ["python", "kp_historical_company.py"]

