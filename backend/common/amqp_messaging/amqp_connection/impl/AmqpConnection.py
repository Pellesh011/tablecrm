import asyncio
from typing import Dict

from aio_pika import connect_robust
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

from ...models.RabbitMqSettings import RabbitMqSettings


class AmqpConnection:

    def __init__(self, settings: RabbitMqSettings):
        self.__settings: RabbitMqSettings = settings
        self.__connection: AbstractRobustConnection | None = None

        self._channels: Dict[int, AbstractRobustChannel] = {}

    async def install(self):
        connection = await connect_robust(
            host=self.__settings.rabbitmq_host,
            port=self.__settings.rabbitmq_port,
            login=self.__settings.rabbitmq_user,
            password=self.__settings.rabbitmq_pass,
            virtualhost=self.__settings.rabbitmq_vhost,
            loop=asyncio.get_running_loop(),
        )
        self.__connection = connection

    async def get_channel(self) -> AbstractRobustChannel:
        if not self.__connection:
            raise Exception("You are not connected to AMQP. Use install().")

        channel = await self.__connection.channel()
        self._channels[len(self._channels) + 1] = channel
        return channel

    async def close(self) -> None:
        for channel_id, channel in list(self._channels.items()):
            try:
                if channel and not channel.is_closed:
                    await channel.close()
            except Exception:
                pass
            finally:
                self._channels.pop(channel_id, None)

        if self.__connection and not self.__connection.is_closed:
            try:
                await self.__connection.close()
            except Exception:
                pass

        self.__connection = None
