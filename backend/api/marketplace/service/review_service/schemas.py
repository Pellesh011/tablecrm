from datetime import datetime
from enum import Enum
from typing import List, Optional

from api.marketplace.service.review_service.constants import ReviewEntityTypes
from pydantic import BaseModel, Field


class CreateReviewRequest(BaseModel):
    """Запрос на создание отзыва"""

    entity_type: ReviewEntityTypes
    entity_id: int
    rating: int = Field(ge=1, le=5)  # 1-5
    text: str
    contragent_phone: str


class MarketplaceReview(BaseModel):
    """Ответ с отзывом"""

    id: int
    entity_type: ReviewEntityTypes
    entity_id: int
    rating: int = Field(ge=1, le=5)  # 1-5
    text: str
    status: str  # pending, visible, hidden
    created_at: datetime
    updated_at: datetime
    client_id: int

    class Config:
        orm_mode = True


class ReviewListResponse(BaseModel):
    """Список отзывов"""

    result: List[MarketplaceReview]
    count: int
    page: int
    size: int
    avg_rating: Optional[float] = None


class ReviewListRequest(BaseModel):
    class SortBy(Enum):
        newest = "newest"
        oldest = "oldest"
        highest = "highest"
        lowest = "lowest"

    entity_type: ReviewEntityTypes
    entity_id: int
    page: Optional[int] = 1
    size: Optional[int] = 20
    sort: Optional[SortBy] = SortBy.newest
    view_only_rates: Optional[int] = Field(default=None, ge=1, le=5)


class UpdateReviewRequest(BaseModel):
    contragent_phone: str
    rating: int = Field(ge=1, le=5)
    text: str
