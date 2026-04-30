import re
from typing import Optional

from common.apple_wallet_service.impl.models import Location, PassColorConfig
from pydantic import BaseModel, validator


class WalletCardSettings(BaseModel):
    logo_text: str = ""
    description: str = ""
    barcode_message: str = "TableCRM"

    colors: Optional[PassColorConfig] = PassColorConfig(
        backgroundColor="#3875f6", foregroundColor="#ffffff", labelColor="#ffffff"
    )
    # Пути могут быть как локальными (начинаются с /), так и S3 ключами
    icon_path: Optional[str] = "photos/AppleWalletIconDefault.png"
    logo_path: Optional[str] = "photos/AppleWalletLogoDefault.png"
    strip_path: Optional[str] = "photos/AppleWalletStripDefault.png"

    locations: list[Location] = []

    @validator("barcode_message")
    def validate_message(cls, value):
        pattern = re.compile(r"^[A-Za-z0-9]+$")
        if pattern.match(value):
            return value
        return "TableCRM"


class WalletCardSettingsCreate(BaseModel):
    cashbox_id: int
    data: WalletCardSettings


class WalletCardSettingsUpdate(WalletCardSettingsCreate):
    data: dict
