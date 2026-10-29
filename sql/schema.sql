CREATE TABLE IF NOT EXISTS dim_assets (
    id UINTEGER PRIMARY KEY,
    ticker VARCHAR,
    market_type VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_time (
    timestamp UBIGINT PRIMARY KEY,
    year UINTEGER,
    month UINTEGER,
    day UINTEGER,
    hour UINTEGER,
    minute UINTEGER,
    day_of_week VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_stream_metrics (
    timestamp UBIGINT,
    asset_id UINTEGER,
    value DOUBLE,
    moving_average DOUBLE,
    is_anomaly BOOLEAN,
    FOREIGN KEY (asset_id) REFERENCES dim_assets(id),
    FOREIGN KEY (timestamp) REFERENCES dim_time(timestamp)
);