from typing import Optional

from database.db import BookingEventStatus
from pydantic import BaseModel


class BaseBookingEventModel(BaseModel):
    booking_nomenclature_id: int
    type: BookingEventStatus
    value: Optional[str]
    latitude: str
    longitude: str
