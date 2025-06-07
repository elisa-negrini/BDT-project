FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p model 

COPY create_model_lstm/ create_model_lstm/

COPY prediction_layer.py /app

CMD ["python", "prediction_layer.py"]