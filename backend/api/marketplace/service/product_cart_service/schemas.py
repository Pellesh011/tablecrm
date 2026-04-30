from datetime import datetime
from typing import List, Optional

from api.marketplace.service.orders_service.schemas import MarketplaceOrderGood
from pydantic import BaseModel


class MarketplaceGetCartRequest(BaseModel):
    contragent_phone: str


class MarketplaceOrderGoodCustom(MarketplaceOrderGood):
    is_from_cart: Optional[bool] = True


class MarketplaceAddToCartRequest(BaseModel):
    contragent_phone: str
    good: MarketplaceOrderGoodCustom


class MarketplaceCartResponse(BaseModel):
    contragent_phone: str
    goods: List[MarketplaceOrderGood]
    total_count: int


class MarketplaceRemoveFromCartRequest(BaseModel):
    contragent_phone: str
    nomenclature_id: int
    warehouse_id: Optional[int] = None


class MarketplaceCartGoodRepresentation(BaseModel):
    id: int
    nomenclature_id: int
    warehouse_id: Optional[int] = None
    quantity: int
    cart_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
