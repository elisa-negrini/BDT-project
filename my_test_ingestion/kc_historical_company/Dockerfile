# historical company/Dockerfile

FROM python:3.9-slim
USER root
RUN pip install --no-cache-dir boto3 pandas kafka-python pyarrow

COPY . /app
WORKDIR /app

CMD ["python", "kc_historical_company.py"]