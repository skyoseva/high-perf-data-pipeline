from fastapi import FastAPI, Depends, HTTPException, Query
import duckdb
import os

app = FastAPI(
    title="Stream Analytics API",
    description="High-performance real-time streaming analytics powered by C++, PyArrow, and DuckDB.",
    version="1.0.0"
)

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../stream_analytics.duckdb"))

def get_db():
    """Dependency that yields a DuckDB connection."""
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=503, detail="Database file not found. Is the pipeline running?")
    
    conn = duckdb.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

@app.get("/api/v1/health")
def health_check(conn = Depends(get_db)):
    """Simple health check endpoint confirming database connectivity."""
    result = conn.execute("SELECT COUNT(*) FROM fact_stream_metrics").fetchone()
    return {"status": "healthy", "total_records_stored": result[0]}

@app.get("/api/v1/metrics/latest")
def get_latest_metrics(limit: int = Query(50, ge=1, le=500), conn = Depends(get_db)):
    """Fetch the most recent stream metrics with asset tickers and anomaly flags."""
    query = """
        SELECT 
            f.timestamp,
            da.ticker,
            da.market_type,
            f.value,
            f.moving_average,
            f.is_anomaly
        FROM fact_stream_metrics f
        JOIN dim_assets da ON f.asset_id = da.id
        ORDER BY f.timestamp DESC
        LIMIT ?
    """
    rows = conn.execute(query, [limit]).fetchall()
    
    # Map to clean JSON response
    return [
        {
            "timestamp": row[0],
            "ticker": row[1],
            "market_type": row[2],
            "value": row[3],
            "moving_average": row[4],
            "is_anomaly": row[5]
        }
        for row in rows
    ]

@app.get("/api/v1/analytics/anomalies")
def get_anomalies(ticker: str = None, limit: int = Query(20, ge=1, le=100), conn = Depends(get_db)):
    """Fetch historical anomaly events, optionally filtered by asset ticker."""
    base_query = """
        SELECT 
            f.timestamp,
            da.ticker,
            f.value,
            f.moving_average
        FROM fact_stream_metrics f
        JOIN dim_assets da ON f.asset_id = da.id
        WHERE f.is_anomaly = TRUE
    """
    params = []
    if ticker:
        base_query += " AND da.ticker = ?"
        params.append(ticker.upper())
        
    base_query += " ORDER BY f.timestamp DESC LIMIT ?"
    params.append(limit)
    
    rows = conn.execute(base_query, params).fetchall()
    
    return [
        {
            "timestamp": row[0],
            "ticker": row[1],
            "value": row[2],
            "moving_average": row[3]
        }
        for row in rows
    ]

@app.get("/api/v1/analytics/candles")
def get_ohlcv_candles(
    ticker: str = Query(..., description="Asset ticker symbol, e.g. BTCUSDT"),
    interval_seconds: int = Query(60, description="Bucket size in seconds (default 60s / 1 min)"),
    limit: int = Query(50, ge=1, le=500, description="Number of candles to return"),
    conn = Depends(get_db)
):
    """
    Resamples raw tick metrics into time-bucketed OHLCV (Open, High, Low, Close) 
    candlesticks using DuckDB native aggregations.
    """
    query = """
        WITH candles AS (
            SELECT
                da.ticker,
                CAST((f.timestamp / ?) * ? AS BIGINT) AS bucket_timestamp,
                arg_min(f.value, f.timestamp) AS open_price,
                MAX(f.value) AS high_price,
                MIN(f.value) AS low_price,
                arg_max(f.value, f.timestamp) AS close_price,
                COUNT(*) AS tick_count
            FROM fact_stream_metrics f
            JOIN dim_assets da ON f.asset_id = da.id
            WHERE da.ticker = ?
            GROUP BY da.ticker, bucket_timestamp
        )
        SELECT
            ticker,
            bucket_timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
            tick_count,
            -- Previous candle's close price for return calculation
            LAG(close_price, 1) OVER w AS prev_close,
            -- Percentage return from the previous candle's close
            (close_price - LAG(close_price, 1) OVER w) / NULLIF(LAG(close_price, 1) OVER w, 0) AS candle_return,
            -- 5-candle rolling simple moving average of close_price (current + 4 prior buckets)
            AVG(close_price) OVER (
                PARTITION BY ticker
                ORDER BY bucket_timestamp ASC
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS sma_5_close
        FROM candles
        WINDOW w AS (PARTITION BY ticker ORDER BY bucket_timestamp ASC)
        ORDER BY bucket_timestamp DESC
        LIMIT ?;
    """
    
    # Parameters map safely to the query placeholders to prevent injection
    params = [
        interval_seconds, 
        interval_seconds, 
        ticker.upper(), 
        limit
    ]
    
    rows = conn.execute(query, params).fetchall()
    
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for ticker '{ticker}'")

    return [
        {
            "ticker": row[0],
            "bucket_timestamp": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "tick_count": row[6],
            "prev_close": row[7],
            "candle_return": row[8],
            "sma_5_close": row[9]
        }
        for row in rows
    ]