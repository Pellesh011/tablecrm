import dataclasses
from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from api.docs_sales.messages.RecalculateFinancialsMessageModel import (
    RecalculateFinancialsMessageModel,
)
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from functions.users import raschet


class RecalculateFinancialsHandler(IEventHandler[RecalculateFinancialsMessageModel]):

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        recalculate_financials_message_model = RecalculateFinancialsMessageModel(
            **event
        )

        @dataclasses.dataclass
        class User:
            cashbox_id: int

        await raschet(
            user=User(cashbox_id=recalculate_financials_message_model.cashbox_id),
            token=recalculate_financials_message_model.token,
        )
