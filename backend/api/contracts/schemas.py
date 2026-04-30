from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class PaymentType(str, Enum):
    docs_sales = "Документ продажи"
    contract = "Договор"


class PaymentTime(str, Enum):
    before_shipping = "До отгрузки"
    after_shipping = "После отгрузки"
    before_shipping_delayed = "До отгрузки с отсрочкой"
    after_shipping_delayed = "После отгрузки с отсрочкой"


class ContractCreate(BaseModel):
    number: str
    name: Optional[str]
    print_name: Optional[str]
    dated: Optional[int]
    used_from: Optional[int]
    used_to: Optional[int]
    status: bool = True
    contragent: Optional[int]
    organization: Optional[int]
    payment_type: Optional[PaymentType]
    payment_time: Optional[PaymentTime]

    class Config:
        orm_mode = True


class ContractCreateMass(BaseModel):
    __root__: List[ContractCreate]

    class Config:
        orm_mode = True


class ContractEdit(ContractCreate):
    number: Optional[str]
    status: Optional[bool]


class Contract(ContractCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class ContractList(BaseModel):
    __root__: Optional[List[Contract]]

    class Config:
        orm_mode = True


class ContractListGet(BaseModel):
    result: Optional[List[Contract]]
    count: int
