from typing import Dict

from aio_pika.abc import AbstractRobustChannel

from ...amqp_channels.impl.RabbitChannel import RabbitChannel
from ...amqp_connection.impl.AmqpConnection import AmqpConnection
from ...common.core.IRabbitFactory import IRabbitFactory
from ...common.core.IRabbitMessaging import IRabbitMessaging
from ...common.impl.RabbitMessagingImpl import RabbitMessagingImpl
from ...models.RabbitMqSettings import RabbitMqSettings


class RabbitFactory(IRabbitFactory):
    def __init__(self, settings: RabbitMqSettings):
        self.__settings = settings
        self._cached_instance = None

    async def __call__(self) -> IRabbitFactory:
        if self._cached_instance is None:
            amqp_connection = AmqpConnection(settings=self.__settings)
            await amqp_connection.install()

            channels: Dict[str, AbstractRobustChannel] = {}
            channels["publication"] = await amqp_connection.get_channel()

            rabbit_channel = RabbitChannel(
                channels=channels, amqp_connection=amqp_connection
            )
            rabbit_messaging = RabbitMessagingImpl(channel=rabbit_channel)

            class RabbitMessageImpl(IRabbitFactory):
                def __init__(self, messaging: IRabbitMessaging):
                    self._messaging = messaging

                async def __call__(self) -> IRabbitMessaging:
                    return self._messaging

                async def publish(self, *args, **kwargs):
                    return await self._messaging.publish(*args, **kwargs)

                async def subscribe(self, *args, **kwargs):
                    return await self._messaging.subscribe(*args, **kwargs)

                async def install(self, *args, **kwargs):
                    return await self._messaging.install(*args, **kwargs)

                async def close(self):
                    if hasattr(self._messaging, "close"):
                        return await self._messaging.close()

            self._cached_instance = RabbitMessageImpl(rabbit_messaging)

        return self._cached_instance
