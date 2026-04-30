from typing import Dict

from aio_pika.abc import AbstractRobustChannel

from ...amqp_channels.core.IRabbitChannel import IRabbitChannel
from ...amqp_connection.impl.AmqpConnection import AmqpConnection


class RabbitChannel(IRabbitChannel):

    def __init__(
        self,
        channels: Dict[str, AbstractRobustChannel],
        amqp_connection: AmqpConnection,
    ):
        self.__channels: Dict[str, AbstractRobustChannel] = channels
        self.__amqp_connection = amqp_connection

    async def get_consumption_channel(self) -> AbstractRobustChannel:
        new_consumption_channel = await self.__amqp_connection.get_channel()
        self.__channels[f"consumption_{len(self.__channels) + 1}"] = (
            new_consumption_channel
        )
        return new_consumption_channel

    async def get_publication_channel(self) -> AbstractRobustChannel:
        return self.__channels.get("publication")

    async def close(self) -> None:
        for channel_name, channel in list(self.__channels.items()):
            try:
                if channel and not channel.is_closed:
                    await channel.close()
            except Exception:
                pass
            finally:
                self.__channels.pop(channel_name, None)

        await self.__amqp_connection.close()
