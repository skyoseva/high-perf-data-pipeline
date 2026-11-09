import asyncio
import json
import time
import websockets
from pipeline import AnalyticalPipeline
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import fast_processor

async def run_stream(symbol:str, asset_id:int, queue: asyncio.Queue, batch_size:int = 50, flush_interval:float = 1.0):

    BINANCE_WS_URL = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
    processor = fast_processor.WindowStreamProcessor(window_size=20, threshold_multiplier=2.5)
    
    reconnect_delay = 2  # seconds
    max_delay = 60  # seconds
    last_trade_id = -1  # Initialize with an invalid trade ID
    raw_buffer = []
    last_flush_time = time.time()

    async def flush_buffer():
        nonlocal raw_buffer, last_flush_time
        if not raw_buffer:
            return
        packets = [fast_processor.DataPacket(ts, asset_id, price) for ts, price in raw_buffer]
        results = processor.process_stream(packets)
        await queue.put((packets, results))
        raw_buffer.clear()
        last_flush_time = time.time()
            
    while True:
        reconnect_delay = 2  # Reset delay on successful connection
        try:
            print(f"Connecting to Binance WebSocket stream for {symbol}...")
            async with websockets.connect(BINANCE_WS_URL) as websocket:
                while True:
                    try:
                        elapsed_time = time.time() - last_flush_time
                        timeout = max(0.05, flush_interval - elapsed_time)
                        message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                        data = json.loads(message)

                        trade_id = data["t"]
                        if trade_id <= last_trade_id:
                            continue  # Skip this trade as it's a duplicate or out of order
                        last_trade_id = trade_id  # Update the last processed trade ID

                        timestamp_ms = int(data["T"])
                        timestamp_sec = timestamp_ms // 1000
                        price = float(data["p"])

                        raw_buffer.append((timestamp_sec, price))

                        if len(raw_buffer) >= batch_size:
                            await flush_buffer()

                    except asyncio.TimeoutError:
                        if not raw_buffer:
                            last_flush_time = time.time()  # Update last flush time even if no data was received
                        elif (time.time() - last_flush_time) >= flush_interval:
                            await flush_buffer()
                        continue  # Continue to the next iteration to check for new messages or flush 
        except websockets.exceptions.ConnectionClosed:
            print("The connection with the WebSocket was closed. Attempting to reconnect...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)  # Exponential backoff
        except Exception as e:
            print(f"An error occurred: {e}")
            break

async def pipeline_writer(queue: asyncio.Queue, db_path: str):
    pipeline = AnalyticalPipeline(db_path)
    pipeline.register_asset(1, "AAPL", "stock")
    pipeline.register_asset(2, "ETHUSDT", "CRYPTO")
    pipeline.register_asset(3, "BTCUSDT", "CRYPTO")

    try:
        while True:
            try:
                packets, results = await queue.get()
                pipeline.process_and_store(packets, results)
                print(f"💾 [DuckDB] Successfully committed batch of {len(packets)} rows.")
            except Exception as db_err:
                print(f"🚨 [DuckDB ERROR] Failed to write batch: {db_err}")
            queue.task_done()
    except asyncio.CancelledError:
        print("Pipeline writer shutting down. Draining remaining queue items...")
        while not queue.empty():
            try:
                packets, results = queue.get_nowait()
                pipeline.process_and_store(packets, results)
                queue.task_done()
            except Exception as e:
                print(f"🚨 [DuckDB ERROR] during shutdown drain: {e}")
        print("Pipeline writer task cancelled. Closing the database connection.")
        pipeline.close()
        raise

async def main():
    db_path = "stream_analytics.duckdb"
    queue = asyncio.Queue()

    writer_task = asyncio.create_task(pipeline_writer(queue, db_path))

    try:
        await asyncio.gather(
            run_stream("AAPL", 1, queue),
            run_stream("BTCUSDT", 3, queue),
            run_stream("ETHUSDT", 2, queue)
        )
    finally:
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stream processing interrupted by user.")
