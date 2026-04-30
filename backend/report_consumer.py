import asyncio
import json
import logging
from os import environ

import aio_pika
from const import report_queue_name
from database.db import database
from functions.goods_distribution import distribute
from functions.gross_profit import generate_gross_profit_report


async def report_consumer() -> None:
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(
        host=environ.get("RABBITMQ_HOST"),
        port=int(environ.get("RABBITMQ_PORT")),
        timeout=10,
    )
    queue_name = report_queue_name
    await database.connect()
    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Will take no more than 100 messages in advance
        await channel.set_qos(prefetch_count=100)

        # Declaring queue
        queue = await channel.declare_queue(queue_name)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                data = json.loads(message.body.decode())
                func_name = data.pop("func_name")
                if func_name == distribute.__name__:
                    await distribute(**data)
                elif func_name == generate_gross_profit_report.__name__:
                    await generate_gross_profit_report(**data)
                else:
                    raise ValueError(f"{func_name} is not a valid function name")


if __name__ == "__main__":
    asyncio.run(report_consumer())
