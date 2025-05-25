# kp historical macrodata / Dockerfile

FROM python:3.9-slim
USER root
RUN pip install --no-cache-dir boto3 pandas kafka-python requests

COPY . /app
WORKDIR /app

CMD ["python", "kp_historical_macrodata.py"]