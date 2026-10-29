import pytest
import fast_processor
from src.python.pipeline import AnalyticalPipeline

@pytest.fixture
def pipeline():
    pipe = AnalyticalPipeline(":memory:")
    yield pipe
    pipe.close()

def test_process_and_store(pipeline):

    pipeline.register_asset(1, "AAPL", "stock")

    packets = [fast_processor.DataPacket(1761058800 + i, 1, 1000 + i) for i in range(10)]
    packets.append(fast_processor.DataPacket(1761058800 + 10, 1, 3))
    packets.append(fast_processor.DataPacket(1761058800 + 11, 1, 5))

    processor = fast_processor.StreamProcessor(window_size=5, threshold_multiplier=2.5)
    results = processor.process_stream(packets)

    pipeline.process_and_store(packets, results)

    packets_count = pipeline.conn.execute("SELECT COUNT(*) FROM fact_stream_metrics").fetchone()
    assert packets_count[0] == len(packets) #Ensure all packets are stored in database

    anomaly_count = pipeline.conn.execute("SELECT COUNT(*) FROM fact_stream_metrics WHERE is_anomaly = TRUE").fetchone()[0]
    print(f"Anomalies found: {anomaly_count}")

    query_result = pipeline.conn.execute("""SELECT f.value, f.is_anomaly, da.ticker, da.market_type, dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.day_of_week
                                            FROM fact_stream_metrics f 
                                            JOIN dim_assets da ON f.asset_id = da.id
                                            JOIN dim_time dt ON f.timestamp = dt.timestamp
                                            WHERE f.is_anomaly = TRUE
                                            """).fetchall()

    for row in query_result:
        print(row)

    assert len(query_result) > 0
    assert query_result[0][0] == 3  # Check the value of the anomaly
    assert query_result[0][2] == "AAPL"  # Check the ticker symbol of the anomaly
    assert query_result[0][3] == "stock"  # Check the market type of the anomaly


    