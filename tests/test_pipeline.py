import pytest
import fast_processor
from src.python.pipeline import AnalyticalPipeline

@pytest.fixture
def pipeline():
    pipe = AnalyticalPipeline(":memory:")
    yield pipe
    pipe.close()

@pytest.mark.parametrize("processortype", [
    fast_processor.WindowStreamProcessor,
    fast_processor.EwmaStreamProcessor
])

def test_process_and_store(pipeline, processortype):

    pipeline.register_asset(1, "AAPL", "stock")

    packets = [fast_processor.DataPacket(1761058800 + i, 1, 1000 + i) for i in range(100)]
    packets.append(fast_processor.DataPacket(1761058800 + 100, 1, 3))
    packets.append(fast_processor.DataPacket(1761058800 + 11, 1, 5))

    processor = processortype()
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
    #assert query_result[1][0] == 3  # Check the value of the anomaly
    assert query_result[0][2] == "AAPL"  # Check the ticker symbol of the anomaly
    assert query_result[0][3] == "stock"  # Check the market type of the anomaly

def test_same_second_timestamp_collision(pipeline):
    pipeline.register_asset(1, "BTCUSDT", "CRYPTO")
    
    # Two different trades arriving in the exact same second
    timestamp = 1761058800
    packets = [
        fast_processor.DataPacket(timestamp, 1, 50000.0),
        fast_processor.DataPacket(timestamp, 1, 50001.0)
    ]
    
    processor = fast_processor.WindowStreamProcessor()
    results = processor.process_stream(packets)

    # Should insert successfully without crashing on dim_time primary key collision
    pipeline.process_and_store(packets, results)

    count = pipeline.conn.execute("SELECT COUNT(*) FROM dim_time WHERE timestamp = ?", (timestamp,)).fetchone()[0]
    assert count == 1  # dim_time should deduplicate the shared timestamp
    
    fact_count = pipeline.conn.execute("SELECT COUNT(*) FROM fact_stream_metrics").fetchone()[0]
    assert fact_count == 2  # both trade facts should still be recorded

def test_process_and_store_validation_errors(pipeline):
    # Test empty inputs
    with pytest.raises(ValueError, match="Packets and results must not be empty"):
        pipeline.process_and_store([], [])

    # Test length mismatch
    packet1 = fast_processor.DataPacket(1761058800, 1, 100.0)
    packet2 = fast_processor.DataPacket(1761058801, 2, 122.0)
    processor = fast_processor.WindowStreamProcessor()
    result = processor.process_stream([packet1])

    with pytest.raises(ValueError, match="Packets and results must have the same length"):
        pipeline.process_and_store([packet1, packet2], result)

def test_multi_asset_batch_storage(pipeline):
    pipeline.register_asset(1, "BTCUSDT", "CRYPTO")
    pipeline.register_asset(2, "ETHUSDT", "CRYPTO")

    packets = [
        fast_processor.DataPacket(1761058800, 1, 60000.0),
        fast_processor.DataPacket(1761058801, 2, 3000.0),
    ]
    
    processor = fast_processor.WindowStreamProcessor()
    results = processor.process_stream(packets)

    pipeline.process_and_store(packets, results)

    tickers = pipeline.conn.execute("""
        SELECT DISTINCT da.ticker 
        FROM fact_stream_metrics f 
        JOIN dim_assets da ON f.asset_id = da.id
        ORDER BY da.ticker""").fetchall()
    
    # Should find both tickers in the stored metrics
    assert len(tickers) == 2