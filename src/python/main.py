import asyncio
import json
import websockets
from pipeline import AnalyticalPipeline
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import fast_processor

async def run_stream(symbol:str, asset_id:int, pipeline:AnalyticalPipeline):

    BINANCE_WS_URL = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
    
    pipeline.register_asset(asset_id, symbol.lower(), "CRYPTO")

    processor = fast_processor.StreamProcessor(window_size=20, threshold_multiplier=2.5)
    reconnect_delay = 2  # seconds
    max_delay = 60  # seconds
    last_trade_id = -1  # Initialize with an invalid trade ID
    while True:
        reconnect_delay = 2  # Reset delay on successful connection
        try:
            print(f"Connecting to Binance WebSocket stream for {symbol}...")
            async with websockets.connect(BINANCE_WS_URL) as websocket:
                while True:
                    try:
                        # Получаваме съобщение от борсата в реално време
                        message = await asyncio.wait_for(websocket.recv(), timeout=10)
                        data = json.loads(message)

                        trade_id = data["t"]
                        if trade_id <= last_trade_id:
                            continue  # Skip this trade as it's a duplicate or out of order
                        last_trade_id = trade_id  # Update the last processed trade ID

                        timestamp_ms = int(data["T"])
                        timestamp_sec = timestamp_ms // 1000
                        price = float(data["p"])

                        packet = fast_processor.DataPacket(timestamp_sec, asset_id, price)

                        results = processor.process_stream([packet])

                        pipeline.process_and_store([packet], results)

                        res = results[0]
                        status = "🚨 ANOMALY!" if res.is_anomaly else "OK"
                        print(f"[{symbol}][Trade ID: {trade_id}][{timestamp_sec}] PRICE: {price:.2f} | MA: {res.moving_average:.2f} | {status}")
                    except asyncio.TimeoutError:
                        print("No data received for 10 seconds.")
                        break 
        except websockets.exceptions.ConnectionClosed:
            print("The connection with the WebSocket was closed. Attempting to reconnect...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)  # Exponential backoff
        except Exception as e:
            print(f"An error occurred: {e}")
            break

async def main():
    pipeline = AnalyticalPipeline("stream_analytics.duckdb")

    await asyncio.gather(
        run_stream("BTCUSDT", 1, pipeline),
        run_stream("ETHUSDT", 2, pipeline)
    )

    pipeline.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stream processing interrupted by user.")
