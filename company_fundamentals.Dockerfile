# Usa un'immagine base Python
FROM python:3.10-slim

# Copia lo script nel container
COPY postgresql/companies_info.csv /app/postgresql/companies_info.csv
COPY company_fundamentals.py /app/company_fundamentals.py

RUN pip install --no-cache-dir pandas requests pyarrow

# Imposta la working directory
WORKDIR /app

# Esegui lo script quando il container parte
CMD ["python", "company_fundamentals.py"]