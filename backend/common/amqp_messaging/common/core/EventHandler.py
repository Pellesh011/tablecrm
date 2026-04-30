from typing import Generic, Optional, TypeVar

from aio_pika import IncomingMessage
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage

E = TypeVar("E", bound=BaseModelMessage)


class IEventHandler(Generic[E]):

    async def __call__(self, event: E, message: Optional[IncomingMessage] = None):
        raise NotImplementedError()
