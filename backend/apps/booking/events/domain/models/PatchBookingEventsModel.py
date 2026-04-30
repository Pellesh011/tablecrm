from typing import Optional

from database.db import BookingEventStatus
from pydantic import BaseModel


class PatchBookingEventsModel(BaseModel):
    id: int
    type: Optional[BookingEventStatus]
    value: Optional[str]
    latitude: Optional[str]
    longitude: Optional[str]
