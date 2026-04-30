import os

from aioapns import APNs, NotificationRequest
from common.apple_wallet_service.IWalletNotificationService import (
    IWalletNotificationService,
)
from database.db import apple_push_tokens, database
from sqlalchemy import select


class WalletNotificationService(IWalletNotificationService):
    def __init__(self):
        with open(os.getenv("APPLE_NOTIFICATION_PATH")) as key:
            self.__client = APNs(
                key=key.read().strip(),
                key_id=os.getenv("APPLE_NOTIFICATION_KEY"),
                team_id=os.getenv("APPLE_TEAM_ID"),
                topic=os.getenv("APPLE_PASS_TYPE_ID"),
                use_sandbox=False,
            )

    async def renew_pass(self, push_token: str):
        request = NotificationRequest(device_token=push_token, message={})

        await self.__client.send_notification(request)

    async def ask_update_pass(self, card_id: int) -> bool:
        try:
            query = select(apple_push_tokens.c.push_token).where(
                apple_push_tokens.c.card_id == card_id
            )
            push_tokens = await database.fetch_all(query)

            for token in push_tokens:
                await self.renew_pass(token.push_token)
            return True
        except AttributeError:  # карта не зарегистрирована
            return False
