from datetime import date
from enum import Enum
from typing import Optional

from common.schemas import RuPhone
from database.enums import ContragentType, Gender
from pydantic import BaseModel


class Contragent_types(str, Enum):
    Supplier = "Поставщик"
    Buyer = "Покупатель"


class Contragent(BaseModel):
    name: str
    phone: Optional[RuPhone]
    inn: Optional[str]
    description: Optional[str]
    contragent_type: Optional[Contragent_types]
    birth_date: Optional[date]
    data: Optional[dict]
    gender: Optional[Gender] = None
    type: Optional[ContragentType] = None
    additional_phones: Optional[RuPhone]

    class Config:
        arbitrary_types_allowed = True


class ContragentEdit(Contragent):
    name: Optional[str]
    external_id: Optional[str]
    tags_id: Optional[list[int]]


class ContragentCreate(Contragent):
    name: str
    external_id: Optional[str]
    phone: Optional[RuPhone]
    inn: Optional[str]
    description: Optional[str]
    tags_id: Optional[list[int]]

    class Config:
        arbitrary_types_allowed = True


class ContragentResponse(Contragent):
    external_id: Optional[str]
