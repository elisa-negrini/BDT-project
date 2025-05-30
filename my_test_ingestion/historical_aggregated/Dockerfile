# Usa un'immagine Flink di base
FROM flink:1.17-scala_2.12

# Installa Python, pip e le dipendenze di build per psycopg2
# psycopg2 richiede header di sviluppo per PostgreSQL
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Crea un link simbolico per python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Installa le librerie Python
# Aggiunto psycopg2-binary
RUN pip3 install \
    apache-flink==1.17.0 \
    numpy \
    pandas \
    python-dateutil \
    pytz \
    sqlalchemy \
    psycopg2-binary

# Copia lo script Python
COPY historical_aggregated.py /opt/historical_aggregated.py

# Copia i JAR per Kafka (essenziali per il connettore Kafka di Flink)
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/

# Imposta il comando di default per eseguire lo script Flink
CMD ["python", "/opt/historical_aggregated.py"]
