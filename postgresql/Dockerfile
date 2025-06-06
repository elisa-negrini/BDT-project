FROM  postgres:16-alpine
# postgres:latest

# Copia lo script SQL che crea la tabella e carica i dati
COPY create_db.sql /docker-entrypoint-initdb.d/

# Copia il file CSV con i dati dei ticker/aziende
COPY companies_info.csv /docker-entrypoint-initdb.d/
