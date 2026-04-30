from datetime import datetime
from typing import List, Optional

from common.utils.validators import sanitize_float
from pydantic import BaseModel, validator


class Payboxes(BaseModel):
    id: int
    external_id: Optional[str]
    name: str
    start_balance: float
    balance: float
    balance_date: int
    created_at: int
    update_start_balance: int
    update_start_balance_date: int
    organization_id: Optional[int]
    created_at: int
    updated_at: int
    deleted_at: Optional[datetime] = None

    @validator("start_balance", "balance", pre=True)
    def validate_float(cls, v):
        return sanitize_float(v) or 0.0

    class Config:
        orm_mode = True


class PayboxesEdit(BaseModel):
    id: int
    external_id: Optional[str]
    start_balance: Optional[float]
    balance_date: Optional[int]
    name: Optional[str]
    organization_id: Optional[int]

    @validator("start_balance", pre=True)
    def validate_float(cls, v):
        return sanitize_float(v)

    class Config:
        orm_mode = True


class PayboxesCreate(BaseModel):
    name: str
    start_balance: float
    external_id: Optional[str]
    organization_id: Optional[int]

    @validator("start_balance", pre=True)
    def validate_float(cls, v):
        return sanitize_float(v) or 0.0

    class Config:
        orm_mode = True


class GetPayments(BaseModel):
    result: Optional[List[Payboxes]]
    count: int

    class Config:
        orm_mode = True


class PayboxesShort(BaseModel):
    id: int
    external_id: Optional[str]
    name: str
    created_at: int
    updated_at: int
    deleted_at: Optional[datetime] = None


class GetPaymentsShort(GetPayments):
    result: Optional[List[PayboxesShort]]
