CREATE TABLE IF NOT EXISTS companies_info (
    ticker_id INT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(100) NOT NULL,
    related_words TEXT,  
    is_active BOOLEAN NOT NULL
);

COPY companies_info (ticker_id, ticker, company_name, related_words, is_active)
FROM '/docker-entrypoint-initdb.d/companies_info.csv'
DELIMITER ','
CSV HEADER
NULL 'null';
