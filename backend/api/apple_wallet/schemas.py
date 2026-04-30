from typing import List

from pydantic import BaseModel


class DeviceRegistration(BaseModel):
    pushToken: str


class WalletCardCreate(BaseModel):
    card_id: int


class SerialNumbersList(BaseModel):
    serialNumbers: List[str]
    lastUpdated: str
