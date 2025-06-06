# Usa un'immagine Flink di base
FROM flink:1.17-scala_2.12

# Installa Python, pip e le dipendenze di build per psycopg2
# psycopg2 richiede header di sviluppo per PostgreSQL
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    libpq-dev \
    gcc && \
    rm -rf /var/lib/apt/lists/*

# Create a symbolic link for python
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY requirements.txt .

# Install Python libraries
# Added psycopg2-binary
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy Kafka JARs (essential for Flink's Kafka connector)
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/

COPY historical_aggregated.py /app/historical_aggregated.py

# Set the default command to run the Flink script
CMD ["python", "historical_aggregated.py"]