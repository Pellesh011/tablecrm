from typing import List, Type

import aiormq
from aio_pika.abc import AbstractRobustChannel

from ...common.core.EventHandler import IEventHandler
from ...models.BaseModelMessage import BaseModelMessage
from ..impl.models.QueueSettingsModel import QueueSettingsModel


class IRabbitMessaging:

    async def publish(
        self,
        message: BaseModelMessage,
        routing_key: str,
        priority: int = None,
        ttl_expiration: int = None,
    ) -> aiormq.abc.ConfirmationFrameType:
        raise NotImplementedError()

    async def subscribe(
        self, event_type: Type[BaseModelMessage], event_handler: IEventHandler
    ):
        raise NotImplementedError()

    async def install(
        self, queues_settings: List[QueueSettingsModel]
    ) -> List[AbstractRobustChannel]:
        raise NotImplementedError()

    async def close(self) -> None:
        raise NotImplementedError()
