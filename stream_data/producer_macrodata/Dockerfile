FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY producer_macrodata.py /app/producer_macrodata.py

CMD ["python", "producer_macrodata.py"]
