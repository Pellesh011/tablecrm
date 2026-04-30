import datetime
import uuid
from typing import Optional

from pydantic import BaseModel, validator


class PassColorConfig(BaseModel):
    backgroundColor: str
    foregroundColor: str
    labelColor: str


class Location(BaseModel):
    longitude: float
    latitude: float
    relevantText: str


class PassParamsModel(BaseModel):
    serial_number: str
    card_number: str  # из loyalty cards
    contragent_name: str
    cashback_persent: str
    organization_name: str
    description: str
    barcode_message: str

    locations: list[Location]

    logo_text: str
    colors: PassColorConfig

    balance: str

    icon_path: str
    logo_path: str
    strip_path: str

    exp_date: Optional[datetime.datetime] = None

    auth_token: str = str(uuid.uuid4())

    advertisement: str = "TableCRM"

    @validator("description")
    def description_validator(cls, value):
        if value:
            return value
        return "TableCRM"

    @validator("barcode_message")
    def barcode_message_validator(cls, value):
        if value:
            return value
        return "TableCRM"
