FROM python:3.10-slim

COPY postgresql/companies_info.csv /app/postgresql/companies_info.csv
COPY company_fundamentals.py /app/company_fundamentals.py

RUN pip install --no-cache-dir pandas requests pyarrow

WORKDIR /app

CMD ["python", "company_fundamentals.py"]