import fast_processor
import src.python.pipeline as pipeline

packets = list()
timestamp = 1761058800
value = 1000
for i in range(10):
    packets.append(fast_processor.DataPacket(timestamp + i, 1, value + i))
packets.append(fast_processor.DataPacket(1761058800 + 10, 1, 3))
packets.append(fast_processor.DataPacket(1761058800 + 11, 1, 5))


processor = fast_processor.StreamProcessor(window_size = 5, threshold_multiplier = 2.5)

result = processor.process_stream(packets)

print(len(result))

#for index, res in enumerate(result, start = 1):
#    print(f"{index}: {res.timestamp}, {res.moving_average}, {res.is_anomaly}")

pipe = pipeline.AnalyticalPipeline(":memory:")
pipe.register_asset(1, "AAPL", "stock")
pipe.process_and_store(packets, result)

query_result = pipe.conn.execute("""SELECT f.value, f.is_anomaly, da.ticker, da.market_type, dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.day_of_week
                                    FROM fact_stream_metrics f 
                                    JOIN dim_assets da ON f.asset_id = da.id
                                    JOIN dim_time dt ON f.timestamp = dt.timestamp
                                    WHERE f.is_anomaly = TRUE
                                    """).fetchall()

for row in query_result:
    print(row)

"""
import cpp_processor 

processor = cpp_processor.DataProcessor()
prices = [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
result = processor.calculate_sma(prices, window = 3)

print("Cpp result: ", result)
"""