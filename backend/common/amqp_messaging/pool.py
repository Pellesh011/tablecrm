# common/amqp_messaging/pool.py
import asyncio
import os
from typing import Optional

import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection


class RabbitConnectionPool:
    _instance: Optional["RabbitConnectionPool"] = None
    _connection: Optional[AbstractRobustConnection] = None
    _channel: Optional[AbstractRobustChannel] = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_channel(cls) -> AbstractRobustChannel:
        async with cls._lock:
            if cls._connection is None or cls._connection.is_closed:
                cls._connection = await aio_pika.connect_robust(
                    host=os.getenv("RABBITMQ_HOST"),
                    port=int(os.getenv("RABBITMQ_PORT")),
                    login=os.getenv("RABBITMQ_USER"),
                    password=os.getenv("RABBITMQ_PASS"),
                    virtualhost=os.getenv("RABBITMQ_VHOST"),
                    heartbeat=60,
                    connection_attempts=3,
                    retry_delay=2,
                )
            if cls._channel is None or cls._channel.is_closed:
                cls._channel = await cls._connection.channel()
                # чтобы consumer не захлёбывался
                await cls._channel.set_qos(prefetch_count=10)
            return cls._channel

    @classmethod
    async def close(cls):
        async with cls._lock:
            if cls._channel and not cls._channel.is_closed:
                await cls._channel.close()
            if cls._connection and not cls._connection.is_closed:
                await cls._connection.close()
