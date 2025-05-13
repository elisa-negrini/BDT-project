# historical alpaca/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    alpaca-py \
    pandas \
    s3fs \
    pyarrow

COPY historical_data_stock.py /app/historical_data_stock.py

CMD ["python", "historical_data_stock.py"]