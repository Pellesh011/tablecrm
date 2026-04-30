from datetime import datetime
from enum import Enum
from typing import List, Optional

from api.marketplace.schemas import BaseMarketplaceUtm, UtmEntityType
from pydantic import BaseModel


class ViewEventEntityType(str):
    pass


class Event(str, Enum):
    view = "view"
    click = "click"


class GetViewEventsRequest(BaseModel):
    cashbox_id: int
    from_time: Optional[datetime] = None
    to_time: Optional[datetime] = None
    contragent_phone: Optional[str] = None
    entity_type: Optional[ViewEventEntityType] = None
    event: Optional[Event] = None


class ViewEvent(BaseModel):
    id: int
    entity_type: ViewEventEntityType
    event: Event
    entity_id: int
    listing_pos: int
    listing_page: int
    client_id: int
    created_at: datetime

    class Config:
        orm_mode = True


class GetViewEventsList(BaseModel):
    events: List[ViewEvent]
    count: int


class CreateViewEventRequest(BaseModel):
    """Запрос на создание события просмотра"""

    entity_type: ViewEventEntityType  # "product" или "location"
    entity_id: int
    listing_pos: Optional[int] = None  # Позиция в выдаче
    listing_page: Optional[int] = None  # Страница выдачи
    event: Event = Event.view  # Событие просмотра
    # utm: Optional[Dict[str, Any]] = None
    contragent_phone: Optional[str] = None  # Для аналитики


class CreateViewEventResponse(BaseModel):
    """Ответ на создание события просмотра"""

    success: bool
    message: str


class ViewEventsUtm(BaseMarketplaceUtm):
    entity_type: UtmEntityType = UtmEntityType.view_events
