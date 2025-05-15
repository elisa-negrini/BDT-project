# historical macrodata/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    pandas \
    s3fs \
    pyarrow \
    boto3 \
    requests

COPY historical_macrodata.py /app/historical_macrodata.py

CMD ["python", "historical_macrodata.py"]