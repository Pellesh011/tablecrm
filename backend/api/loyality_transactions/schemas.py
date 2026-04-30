from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class LoyalityTranstactionFilters(BaseModel):
    type: Optional[str]
    amount: Optional[int]
    loyality_card_number: Optional[int]
    tags: Optional[str]
    name: Optional[str]
    description: Optional[str]

    dated_from: Optional[int]
    dated_to: Optional[int]

    status: Optional[bool]

    # autoburned: Optional[bool]
    updated_at__gte: Optional[int] = None
    updated_at__lte: Optional[int] = None


class LoyalityTranstactionType(str, Enum):
    accrual = "accrual"
    withdraw = "withdraw"


class LoyalityTransactionCreate(BaseModel):
    type: str = "accrual"
    dated: Optional[int]
    amount: Optional[float]
    loyality_card_number: str
    tags: Optional[str] = ""
    name: Optional[str]
    description: Optional[str]
    status: bool = True
    external_id: Optional[str]
    cashier_name: Optional[str]
    percentamount: Optional[float]
    preamount: Optional[float]
    dead_at: Optional[int]
    is_deleted: bool = False

    autoburned: Optional[bool] = False

    class Config:
        orm_mode = True


class LoyalityTransactionGet(LoyalityTransactionCreate):
    id: int
    loyality_card_id: int

    # autoburned: bool

    created_at: int
    updated_at: int


class LoyalityTransaction(LoyalityTransactionCreate):
    id: int
    loyality_card_id: int

    # autoburned: bool

    created_at: int
    updated_at: int
    data: dict


class LoyalityTransactionEdit(BaseModel):
    dated: Optional[int]
    amount: Optional[float]
    type: Optional[str]
    loyality_card_id: Optional[int]
    cashier_id: Optional[int]
    docs_sales_id: Optional[int]
    tags: Optional[str]
    name: Optional[str]
    description: Optional[str]
    status: Optional[bool]
    external_id: Optional[int]
    cashier_name: Optional[str]
    dead_at: Optional[int]
    is_deleted: Optional[bool]

    # autoburned: Optional[bool]


class LoyalityTransactionCreateMass(BaseModel):
    __root__: List[LoyalityTransactionCreate]

    class Config:
        orm_mode = True


class LoyalityTransactionsList(BaseModel):
    __root__: Optional[List[LoyalityTransaction]]

    class Config:
        orm_mode = True


class LoyalityTransactionsListGet(BaseModel):
    __root__: Optional[List[LoyalityTransactionGet]]

    class Config:
        orm_mode = True


class CountRes(BaseModel):
    result: Optional[LoyalityTransactionsListGet]
    count: int
