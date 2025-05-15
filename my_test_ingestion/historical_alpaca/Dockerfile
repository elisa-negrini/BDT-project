# historical alpaca/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    alpaca-py \
    pandas \
    s3fs \
    pyarrow

COPY historical_alpaca.py /app/historical_alpaca.py

CMD ["python", "historical_alpaca.py"]