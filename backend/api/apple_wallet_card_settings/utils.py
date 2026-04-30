import json

from api.apple_wallet_card_settings.schemas import (
    WalletCardSettings,
    WalletCardSettingsCreate,
)
from database.db import apple_wallet_card_settings, database


async def create_default_apple_wallet_setting(cashbox_id: int):
    setting_stmt = (
        apple_wallet_card_settings.insert()
        .values(
            WalletCardSettingsCreate(
                cashbox_id=cashbox_id, data=WalletCardSettings()
            ).dict()
        )
        .returning(apple_wallet_card_settings.c.data)
    )
    settings = await database.execute(setting_stmt)
    return WalletCardSettings(**json.loads(settings))
