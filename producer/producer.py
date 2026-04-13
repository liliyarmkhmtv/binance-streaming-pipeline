import asyncio
import os
import logging
import json

from binance_sdk_spot.spot import (
    Spot,
    SPOT_WS_STREAMS_PROD_URL,
    ConfigurationWebSocketStreams,
)

from confluent_kafka.aio import AIOProducer

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create configuration for the WebSocket Streams
configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=os.getenv("STREAM_URL", SPOT_WS_STREAMS_PROD_URL)
)

# Initialize Spot client
client = Spot(config_ws_streams=configuration_ws_streams)

async def produce_message(p, topic, value):
    delivery_future = await p.produce(topic, value=value)
    await delivery_future

async def run_streams():
    connection = None
    p = AIOProducer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")})

    try:
        connection = await client.websocket_streams.create_connection()

        stream_trades = await connection.agg_trade(
            symbol="bnbusdt",
        )
        stream_trades.on("message", lambda data: asyncio.create_task(
            produce_message(p, 'binance.agg_trade', json.dumps(vars(data)).encode())
        ))

        stream_ticker = await connection.ticker(
            symbol="bnbusdt",
        )
        stream_ticker.on("message", lambda data: asyncio.create_task(
            produce_message(p, 'binance.ticker', json.dumps(vars(data)).encode())
        ))

        await asyncio.sleep(5)
        await stream_trades.unsubscribe()
        await stream_ticker.unsubscribe()
        await p.flush()
    except Exception as e:
        logging.error(f"run_streams() error: {e}")
    finally:
        if connection:
            await connection.close_connection(close_session=True)
        await p.close()


if __name__ == "__main__":
    asyncio.run(run_streams())
