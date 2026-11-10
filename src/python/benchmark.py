import timeit
import datetime
from duckdb import df
from duckdb import df
import pyarrow as pa
from collections import namedtuple
import polars as pl

# 1. Define mock data structures and generate sample data
Packet = namedtuple("Packet", ["timestamp", "asset_id", "value"])
Result = namedtuple("Result", ["timestamp", "moving_average", "is_anomaly"])

NUM_RECORDS = 10_000
base_ts = 1700000000
packets = [Packet(base_ts + i, f"asset_{i % 10}", float(i)) for i in range(NUM_RECORDS)]
results = [Result(base_ts + i, float(i * 1.1), i % 2 == 0) for i in range(NUM_RECORDS)]

# 2. Define approach 2: Row-oriented dicts + from_pylist
def benchmark_pylist():
    time_data = []
    fact_data = []
    for packet, result in zip(packets, results):
        ts = result.timestamp
        dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        time_data.append({
            "timestamp": ts, 
            "year": dt.year, 
            "month": dt.month, 
            "day": dt.day, 
            "hour": dt.hour, 
            "minute": dt.minute, 
            "day_of_week": dt.strftime("%A")
        })
        fact_data.append({
            "timestamp": packet.timestamp, 
            "asset_id": packet.asset_id, 
            "value": packet.value, 
            "moving_average": result.moving_average, 
            "is_anomaly": result.is_anomaly
        })
    time_table = pa.Table.from_pylist(time_data)
    fact_table = pa.Table.from_pylist(fact_data)
    return time_table, fact_table

# 3. Define approach 1: Columnar lists + from_arrays
def benchmark_arrays():
    timestamps = [p.timestamp for p in packets]
    asset_ids = [p.asset_id for p in packets]
    values = [p.value for p in packets]
    moving_averages = [r.moving_average for r in results]
    is_anomalies = [r.is_anomaly for r in results]

    dts = [datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc) for ts in timestamps]
    years = [dt.year for dt in dts]
    months = [dt.month for dt in dts]
    days = [dt.day for dt in dts]
    hours = [dt.hour for dt in dts]
    minutes = [dt.minute for dt in dts]
    days_of_week = [dt.strftime("%A") for dt in dts]

    time_table = pa.Table.from_arrays(
        [timestamps, years, months, days, hours, minutes, days_of_week],
        names=["timestamp", "year", "month", "day", "hour", "minute", "day_of_week"]
    )
    fact_table = pa.Table.from_arrays(
        [timestamps, asset_ids, values, moving_averages, is_anomalies],
        names=["timestamp", "asset_id", "value", "moving_average", "is_anomaly"]
    )
    return time_table, fact_table

def benchmark_polars():
    df = pl.DataFrame({
    "timestamp": [p.timestamp for p in packets],
    "asset_id": [p.asset_id for p in packets],
    "value": [p.value for p in packets],
    "moving_average": [r.moving_average for r in results],
    "is_anomaly": [r.is_anomaly for r in results],
    })

    # 2. Transform time dimensions using Polars' fast Rust-backed expression engine
    df = df.with_columns(
    dt=pl.from_epoch("timestamp", time_unit="s").dt.replace_time_zone("UTC")
        ).with_columns(
            year=pl.col("dt").dt.year(),
            month=pl.col("dt").dt.month(),
            day=pl.col("dt").dt.day(),
            hour=pl.col("dt").dt.hour(),
            minute=pl.col("dt").dt.minute(),
            day_of_week=pl.col("dt").dt.strftime("%A"),
        )

    # 3. Export directly to PyArrow tables (near zero-copy)
    time_table = df.select(["timestamp", "year", "month", "day", "hour", "minute", "day_of_week"]).to_arrow()
    fact_table = df.select(["timestamp", "asset_id", "value", "moving_average", "is_anomaly"]).to_arrow()

    return time_table, fact_table



if __name__ == "__main__":
    iterations = 50
    
    # Run benchmarks
    t_pylist = timeit.timeit(benchmark_pylist, number=iterations)
    t_arrays = timeit.timeit(benchmark_arrays, number=iterations)
    
    print(f"--- Benchmark Results ({NUM_RECORDS} rows, {iterations} runs) ---")
    print(f"pa.Table.from_pylist: {t_pylist:.4f} seconds")
    print(f"pa.Table.from_arrays: {t_arrays:.4f} seconds")
    print(f"Speedup: {t_pylist / t_arrays:.2f}x faster using columnar arrays")