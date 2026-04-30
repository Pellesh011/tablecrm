from common.apple_wallet_service.impl.WalletNotificationService import (
    WalletNotificationService,
)
from common.apple_wallet_service.impl.WalletPassService import (
    WalletPassGeneratorService,
)


async def update_apple_wallet_pass(loyalty_card_id: int):
    apple_notification_service = WalletNotificationService()
    apple_wallet_service = WalletPassGeneratorService()
    await apple_wallet_service.update_pass(loyalty_card_id)
    await apple_notification_service.ask_update_pass(loyalty_card_id)
