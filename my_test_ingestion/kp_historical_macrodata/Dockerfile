# historical alpaca/Dockerfile

FROM python:3.9-slim
USER root
RUN pip install --no-cache-dir alpaca-py boto3 pandas kafka-python

COPY . /app
WORKDIR /app

CMD ["python", "historical_macrodata.py"]