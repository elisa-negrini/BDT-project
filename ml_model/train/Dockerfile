FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive

ENV TF_CPP_MIN_LOG_LEVEL=2

WORKDIR /app

COPY ml_model/train/requirements.txt ./requirements.txt
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY ml_model /app/ml_model

WORKDIR /app/ml_model/train

CMD ["python", "train_lstm.py"]