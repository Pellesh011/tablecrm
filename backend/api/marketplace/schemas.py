from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class MarketplaceLocation(BaseModel):
    """Модель локации для маркетплейса"""

    id: int
    name: str
    address: Optional[str] = None
    cashbox_id: Optional[int] = None
    admin_id: Optional[int] = None
    avg_rating: Optional[float] = None
    reviews_count: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        orm_mode = True


class MarketplaceLocationList(BaseModel):
    """Список локаций маркетплейса"""

    result: List[MarketplaceLocation]
    count: int
    page: int
    size: int


class UtmEntityType(Enum):
    docs_sales = "docs_sales"
    view_events = "view_events"
    favorites = "favorites"


class BaseMarketplaceUtm(BaseModel):
    entity_type: UtmEntityType
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[List[str]] = None
    utm_content: Optional[str] = None
    utm_name: Optional[str] = None
    utm_phone: Optional[str] = None
    utm_email: Optional[str] = None
    utm_leadid: Optional[str] = None
    utm_yclientid: Optional[str] = None
    utm_gaclientid: Optional[str] = None
    ref_user: Optional[str] = None

    class Config:
        orm_mode = True
