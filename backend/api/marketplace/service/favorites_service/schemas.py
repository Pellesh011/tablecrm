from datetime import datetime
from enum import Enum
from typing import Any, List, Optional

from api.marketplace.schemas import BaseMarketplaceUtm, UtmEntityType
from pydantic import BaseModel


class FavoriteRequest(BaseModel):
    """Запрос на добавление в избранное"""

    nomenclature_id: int
    contragent_phone: str
    # UTM параметры (опциональные, могут быть в body или query string)
    utm_term: Optional[str] = None
    ref_user: Optional[str] = None


class FavoritesSortBy(str, Enum):
    name = "name"
    description = "description"
    favorite_created_at = "favorite_created_at"
    product_created_at = "product_created_at"
    seller = "seller"
    price = "price"


class FavoritesGroupBy(str, Enum):
    favorite_created_at = "favorite_created_at"
    product_created_at = "product_created_at"
    seller = "seller"


class FavoritesFilters(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    favorite_created_at_from: Optional[datetime] = None
    favorite_created_at_to: Optional[datetime] = None
    product_created_at_from: Optional[datetime] = None
    product_created_at_to: Optional[datetime] = None
    seller_id: Optional[int] = None
    seller_name: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None


class FavoriteResponse(BaseModel):
    """Ответ с избранным"""

    id: int
    nomenclature_id: int
    phone: str
    created_at: datetime
    updated_at: datetime

    # Обогащённые поля из nomenclature + cashboxes + prices
    name: Optional[str] = None
    description_short: Optional[str] = None
    price: Optional[float] = None
    product_created_at: Optional[datetime] = None
    seller_id: Optional[int] = None
    seller_name: Optional[str] = None
    seller_photo: Optional[str] = None

    class Config:
        orm_mode = True


class FavoriteListResponse(BaseModel):
    """Список избранного"""

    result: List[FavoriteResponse]
    count: int
    page: int
    size: int


class FavoriteGroup(BaseModel):
    """Группа избранного"""

    group_key: str
    group_value: Any
    items: List[FavoriteResponse]
    count: int


class FavoriteGroupedListResponse(BaseModel):
    """Список избранного с группировкой"""

    groups: List[FavoriteGroup]
    total_count: int
    page: int
    size: int


class CreateFavoritesUtm(BaseMarketplaceUtm):
    entity_type: UtmEntityType = UtmEntityType.favorites
