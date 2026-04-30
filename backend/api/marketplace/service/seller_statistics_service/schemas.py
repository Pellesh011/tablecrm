from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class SellerStatisticsItem(BaseModel):
    id: int
    seller_name: Optional[str] = None
    seller_description: Optional[str] = None
    seller_photo: Optional[str] = None

    rating: Optional[float] = None
    reviews_count: Optional[int] = None

    orders_total: Optional[int] = None
    orders_completed: Optional[int] = None

    registration_date: Optional[int] = None
    last_order_date: Optional[datetime] = None

    active_warehouses: int
    total_products: int


class SellerStatisticsResponse(BaseModel):
    sellers: List[SellerStatisticsItem]


class SellerListItem(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    photo: Optional[str] = None
    total_products: Optional[int] = None


class SellersListResponse(BaseModel):
    sellers: List[SellerListItem]
    count: int
