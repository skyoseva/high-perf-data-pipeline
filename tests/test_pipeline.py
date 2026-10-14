import os
import pytest
import cpp_processor
import duckdb

def test_cpp_sma_calculation():
    """Tests if the cpp sma calculation is correct"""

    prices = [10.0, 20.0, 30.0, 40.0]
    window = 3

    processor = cpp_processor.DataProcessor()
    result = processor.calculate_sma(prices, window)

    assert len(result) == 2

    assert result[0] == 20.0
    assert result[1] == 30.0

def test_db_integration():
    """Test DB integration, create table and insert/select"""

    conn=duckdb.connect()

    conn.execute("""CREATE TABLE IF NOT EXISTS asset_metrics(
                    id INTEGER PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    price DOUBLE NOT NULL,
                    sma_3 DOUBLE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    """)

    conn.execute("INSERT INTO asset_metrics (id, symbol, price, sma_3) VALUES (?, ?, ?, ?)", (0, "AAPL", 150.0, None))
    conn.execute("INSERT INTO asset_metrics (id, symbol, price, sma_3) VALUES (?, ?, ?, ?)", (1, "AAPL", 151.1, 152.1))

    df = conn.execute("SELECT * FROM asset_metrics").fetchdf()

    assert len(df) == 2
    assert df.loc[0, "price"] == 150.0
    assert is_none_or_nan(df.loc[0, "sma_3"])
    assert df.loc[1, "sma_3"] == 152.1


def is_none_or_nan(val):
    import math
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    return False
    

