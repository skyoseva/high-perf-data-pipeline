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

        # 1. Extract columnar vectors directly without creating row-level dicts
        timestamps = [p.timestamp for p in packets]
        asset_ids = [p.asset_id for p in packets]
        values = [p.value for p in packets]
        moving_averages = [r.moving_average for r in results]
        is_anomalies = [r.is_anomaly for r in results]

        # 2. Extract time dimensions directly via vector comprehensions
        dts = [datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc) for ts in timestamps]
        years = [dt.year for dt in dts]
        months = [dt.month for dt in dts]
        days = [dt.day for dt in dts]
        hours = [dt.hour for dt in dts]
        minutes = [dt.minute for dt in dts]
        days_of_week = [dt.strftime("%A") for dt in dts]

        # 3. Construct PyArrow Tables directly from column arrays
        time_table = pa.Table.from_arrays(
            [timestamps, years, months, days, hours, minutes, days_of_week],
            names=["timestamp", "year", "month", "day", "hour", "minute", "day_of_week"]
        )

        fact_table = pa.Table.from_arrays(
            [timestamps, asset_ids, values, moving_averages, is_anomalies],
            names=["timestamp", "asset_id", "value", "moving_average", "is_anomaly"]
        )

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

