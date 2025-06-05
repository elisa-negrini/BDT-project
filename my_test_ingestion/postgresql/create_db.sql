CREATE TABLE IF NOT EXISTS companies_info (
    ticker_id INT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(100) NOT NULL,
    company_name2 VARCHAR(100),
    is_active BOOLEAN
);

COPY companies_info (ticker_id, ticker, company_name, company_name2)
FROM '/docker-entrypoint-initdb.d/companies_info.csv'
DELIMITER ','
CSV HEADER;
