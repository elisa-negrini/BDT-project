FROM python:3.9-slim

WORKDIR /app

COPY df_company_fundamentals.parquet ./historical_company/

COPY historical_company.py .

RUN pip install pandas pyarrow minio kafka-python

CMD ["python", "historical_company.py"]

