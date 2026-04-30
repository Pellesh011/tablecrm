from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from api.apple_wallet.messages.AppleWalletCardUpdateMessage import (
    AppleWalletCardUpdateMessage,
)
from api.apple_wallet.utils import update_apple_wallet_pass
from common.amqp_messaging.common.core.EventHandler import IEventHandler


class AppleWalletCardUpdateHandler(IEventHandler[AppleWalletCardUpdateMessage]):
    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        card_id = event["loyality_card_id"]
        await update_apple_wallet_pass(card_id)
