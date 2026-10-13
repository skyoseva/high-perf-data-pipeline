import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import duckdb
import cpp_processor

def run_pipeline():

    print("Initializing database and creating tables...")
    conn = duckdb.connect("market_data.duckdb")

    print("Reading the database schema...")
    with open("sql/schema.sql", "r") as f:
        conn.execute(f.read())

    print("Getting data...")
    raw_prices = [150.0, 152.5, 151.0, 155.0, 158.0, 157.5, 160.0]
    symbol = "AAPL"

    print("Calculating SMA values...")
    processor = cpp_processor.DataProcessor()
    sma = processor.calculate_sma(raw_prices, window = 3)

    print("Loading data into the table...")
    conn.execute("DELETE FROM asset_metrics")

    window = 3

    for i, price in enumerate(raw_prices):
        sma_val = sma[i - window + 1] if i >= window - 1 else None
        conn.execute("INSERT INTO asset_metrics (id, symbol, price, sma_3) VALUES (?, ?, ?, ?)", (i + 1, symbol, raw_prices[i], sma_val))

    print("Running the analysis...")
    query = """SELECT symbol, 
                      id, 
                      price,
                      sma_3,
                      ROUND(price - sma_3, 2) AS diff_from_sma,
                      CASE
                        WHEN price > sma_3 THEN 'BULLISH'
                        WHEN price < sma_3 THEN 'BEARISH'
                        ELSE 'NEUTRAL'
                      END AS signal
                FROM asset_metrics
                WHERE sma_3 IS NOT NULL
            """

    result = conn.execute(query).fetchdf()

    print("Result of the analysis: ")
    print(result.to_string(index=1))

if __name__ == "__main__":
    print("running pipeline")
    run_pipeline()
