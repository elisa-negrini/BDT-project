FROM python:3.10-slim

WORKDIR /app

COPY clean_minio.py .

RUN pip install minio

CMD ["python", "clea_minio.py"]
