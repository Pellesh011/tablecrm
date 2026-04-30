from typing import List, Optional

from common.schemas import RuPhone
from pydantic import BaseModel


class LoyalityCardFilters(BaseModel):
    card_number: Optional[int]
    balance: Optional[float]
    tags: Optional[str]
    income: Optional[int]
    outcome: Optional[int]
    cashback_percent: Optional[int]
    minimal_checque_amount: Optional[int]
    max_percentage: Optional[int]
    created_by_id: Optional[int]

    start_period_from: Optional[int]
    end_period_from: Optional[int]

    start_period_to: Optional[int]
    end_period_to: Optional[int]

    created_at_from: Optional[int]
    created_at_to: Optional[int]

    updated_at_from: Optional[int]
    updated_at_to: Optional[int]

    contragent_name: Optional[str]
    phone_number: Optional[str]
    organization_name: Optional[str]

    # lifetime: Optional[int] # lifetime in seconds

    status_card: Optional[bool]
    updated_at__gte: Optional[int] = None
    updated_at__lte: Optional[int] = None
    created_at__gte: Optional[int] = None
    created_at__lte: Optional[int] = None


class LoyalityCardCreate(BaseModel):
    card_number: Optional[str]
    tags: Optional[str]
    phone_number: Optional[RuPhone]
    contragent_id: Optional[int]
    contragent_name: Optional[str]
    organization_id: Optional[int]
    cashback_percent: Optional[int] = None
    minimal_checque_amount: Optional[int] = None
    max_withdraw_percentage: Optional[int] = None
    start_period: Optional[int] = None
    end_period: Optional[int] = None
    max_percentage: Optional[int] = None
    lifetime: Optional[int]  # lifetime in seconds
    status_card: bool = True
    is_deleted: bool = False
    apple_wallet_advertisement: str = "TableCRM"

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class LoyalityCard(LoyalityCardCreate):
    id: int
    created_at: int
    updated_at: int
    data: dict


class LoyalityCardEdit(BaseModel):
    card_number: Optional[int]
    tags: Optional[str]
    cashback_percent: Optional[int]
    minimal_checque_amount: Optional[int]
    start_period: Optional[int]
    end_period: Optional[int]
    max_percentage: Optional[int]
    max_withdraw_percentage: Optional[int]

    lifetime: Optional[int]  # lifetime in seconds

    status_card: Optional[bool]
    is_deleted: Optional[bool]

    apple_wallet_advertisement: Optional[str]


class LoyalityCardGet(BaseModel):
    id: int
    card_number: int
    tags: Optional[str]
    balance: float
    income: Optional[int]
    outcome: Optional[int]
    contragent_id: Optional[int]
    organization_id: Optional[int]
    contragent: str
    organization: str
    cashback_percent: Optional[int]
    minimal_checque_amount: Optional[int]
    max_withdraw_percentage: Optional[int]
    start_period: Optional[int]
    end_period: Optional[int]
    max_percentage: Optional[int]

    lifetime: Optional[int]  # lifetime in seconds

    apple_wallet_advertisement: str

    status_card: Optional[bool]
    is_deleted: Optional[bool]
    created_at: int
    updated_at: int

    class Config:
        orm_mode = True


class LoyalityCardCreateMass(BaseModel):
    __root__: List[LoyalityCardCreate]

    class Config:
        orm_mode = True


class LoyalityCardsListGet(BaseModel):
    __root__: Optional[List[LoyalityCardGet]]

    class Config:
        orm_mode = True


class LoyalityCardsList(BaseModel):
    __root__: Optional[List[LoyalityCard]]

    class Config:
        orm_mode = True


class CountRes(BaseModel):
    result: Optional[LoyalityCardsListGet]
    count: int
