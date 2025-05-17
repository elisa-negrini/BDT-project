# dati storici Alpaca

FROM bitnami/spark:3.5.0

USER root

RUN install_packages python3-pip

COPY historical_data.py /app/historical_data.py

RUN pip install pandas pyarrow

ENTRYPOINT ["spark-submit", "/app/historical_data.py"]