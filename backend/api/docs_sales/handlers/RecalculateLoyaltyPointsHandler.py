from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from api.docs_sales.messages.RecalculateLoyaltyPointsMessageModel import (
    RecalculateLoyaltyPointsMessageModel,
)
from api.loyality_transactions.routers import raschet_bonuses
from common.amqp_messaging.common.core.EventHandler import IEventHandler


class RecalculateLoyaltyPointsHandler(
    IEventHandler[RecalculateLoyaltyPointsMessageModel]
):

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        recalculate_loyalty_points_message_model = RecalculateLoyaltyPointsMessageModel(
            **event
        )

        for (
            loyalty_card_id
        ) in recalculate_loyalty_points_message_model.loyalty_card_ids:
            await raschet_bonuses(card_id=loyalty_card_id)
