# Usa un'immagine Flink generica, che include il runtime Flink e Scala
# La versione 1.17-scala_2.12 è quella che hai nel tuo docker-compose.yml
FROM flink:1.17-scala_2.12

# Aggiorna i pacchetti e installa Python3 e pip.
# --no-install-recommends serve a ridurre la dimensione dell'immagine finale.
# && rm -rf /var/lib/apt/lists/* pulisce i dati di cache di apt per lo stesso motivo.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Crea un link simbolico da 'python3' a 'python' per compatibilità,
# dato che molti script e tools si aspettano il comando 'python'.
RUN ln -s /usr/bin/python3 /usr/bin/python

# Imposta la directory di lavoro all'interno del container.
WORKDIR /app

# Copia il tuo script Flink nella directory di lavoro del container.
COPY consumer_aggregated.py .

# Installa le dipendenze Python necessarie per il tuo script.
# `apache-flink==1.17.0` è il pacchetto PyFlink che deve corrispondere alla versione di Flink.
# `psycopg2-binary` per PostgreSQL, `python-dateutil` e `pytz` per la gestione delle date/orari.
RUN pip install --no-cache-dir \
    apache-flink==1.17.0 \
    psycopg2-binary \
    python-dateutil \
    pytz

# Copia i JAR dei connettori Kafka nella libreria di Flink.
# Questi JAR sono essenziali per permettere a Flink di interagire con Kafka.
# Assicurati che questi file JAR siano presenti nella stessa directory del Dockerfile.
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/

# Comando predefinito che verrà eseguito quando il container si avvia.
# Usa `flink run -py` per eseguire uno script PyFlink.
# `-py` specifica che si tratta di un job Python e il percorso dello script.
CMD ["/opt/flink/bin/flink", "run", "-py", "/app/consumer_aggregated.py"]