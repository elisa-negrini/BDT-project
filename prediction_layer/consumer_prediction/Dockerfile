FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consumer_prediction.py .

CMD ["python", "consumer_prediction.py"]
