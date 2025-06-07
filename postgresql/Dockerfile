FROM  postgres:16-alpine

COPY create_db.sql /docker-entrypoint-initdb.d/

COPY companies_info.csv /docker-entrypoint-initdb.d/