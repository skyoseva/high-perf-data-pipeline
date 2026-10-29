import os
import sys
import pyarrow as pa
import duckdb
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import fast_processor

class AnalyticalPipeline:
    def __init__(self, db_path: str = "data_pipeline.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(self.db_path)
        self._init_schema()

    def _init_schema(self):
        try:
            with open("sql/schema.sql", "r") as f:
                schema_sql = f.read()
        except (FileNotFoundError, PermissionError) as e:
            raise FileNotFoundError("Schema file not found or not readable at 'sql/schema.sql'. Please check the path.") from e
        try:
            self.conn.execute("BEGIN TRANSACTION;")
            self.conn.execute(schema_sql)
            self.conn.execute("COMMIT;")
        except Exception as e:
            self.conn.execute("ROLLBACK;")
            raise RuntimeError(f"Schema initialization failed: {e}") from e

    def register_asset(self, asset_id: int, ticker: str, market_type: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO dim_assets (id, ticker, market_type) VALUES (?, ?, ?)",
                (asset_id, ticker, market_type)
        )

    def process_and_store(self, packets:list, results:list):
        if not packets or not results:
            raise ValueError("Packets and results must not be empty.")
        if len(packets) != len(results):
            raise ValueError("Packets and results must have the same length.")

        time_data = []
        fact_data = []

        
        for packet, result in zip(packets, results):

            ts = result.timestamp
            dt = datetime.datetime.fromtimestamp(ts)

            time_data.append({ "timestamp": ts, "year": dt.year, "month": dt.month, "day": dt.day, "hour": dt.hour, "minute": dt.minute, "day_of_week": dt.strftime("%A")})
            fact_data.append({ "timestamp": packet.timestamp, "asset_id": packet.asset_id, "value": packet.value, "moving_average": result.moving_average, "is_anomaly": result.is_anomaly })

        time_table = pa.Table.from_pylist(time_data)
        fact_table = pa.Table.from_pylist(fact_data)

        self.conn.execute("BEGIN TRANSACTION;")
        try:
            self.conn.execute("INSERT OR IGNORE INTO dim_time SELECT * FROM time_table")
            self.conn.execute("INSERT INTO fact_stream_metrics SELECT * FROM fact_table")
            self.conn.execute("COMMIT;")
        except Exception as e:
            self.conn.execute("ROLLBACK;")
            raise RuntimeError(f"Failed to insert data into the database: {e}") from e

    def close(self):
        self.conn.close()

