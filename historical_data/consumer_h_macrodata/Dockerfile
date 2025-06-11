FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consumer_h_macrodata.py .

CMD ["python", "consumer_h_macrodata.py"]