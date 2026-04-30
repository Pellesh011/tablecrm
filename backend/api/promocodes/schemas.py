from datetime import datetime
from typing import Optional

from api.promocodes.models import PromocodeType
from pydantic import BaseModel, Field, validator


class PromoActivateRequest(BaseModel):
    code: str = Field(..., min_length=3, description="Промокод")

    phone_number: str = Field(
        ..., min_length=5, description="Номер телефона клиента (7999...)"
    )


class PromoActivateResponse(BaseModel):
    success: bool
    added_points: float
    new_balance: float
    message: str
    transaction_id: int


class PromoCodeBase(BaseModel):
    code: str = Field(..., min_length=3, max_length=50)
    points_amount: float = Field(..., gt=0)
    type: PromocodeType = PromocodeType.PERMANENT
    max_usages: Optional[int] = Field(None, gt=0)
    valid_after: Optional[int] = Field(None, gt=0)
    valid_until: Optional[int] = Field(None, gt=0)
    is_active: bool = True
    organization_id: int
    distributor_id: Optional[int] = None


class PromoCodeCreate(PromoCodeBase):
    pass


class PromoCodeUpdate(BaseModel):
    points_amount: Optional[float] = Field(None, gt=0)
    max_usages: Optional[int] = Field(None, gt=0)
    valid_after: Optional[int] = Field(None, gt=0)
    valid_until: Optional[int] = Field(None, gt=0)
    is_active: Optional[bool] = None


class GetPromoCodeNoRelation(PromoCodeBase):
    id: int
    current_usages: int
    creator_id: int
    created_at: int
    updated_at: Optional[int]
    deleted_at: Optional[int]

    @validator(
        "created_at", "updated_at", "deleted_at", "valid_after", "valid_until", pre=True
    )
    def datetime_to_timestamp(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            return int(v.timestamp())
        return v

    class Config:
        orm_mode = True
