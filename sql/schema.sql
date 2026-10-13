CREATE TABLE if not exists asset_metrics (
    id INTEGER PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price DOUBLE NOT NULL,
    sma_3 DOUBLE,
    create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);