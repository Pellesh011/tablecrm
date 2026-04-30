from typing import List

from pydantic import BaseModel

from .BaseBookingEventModel import BaseBookingEventModel


class ResponseGetBookingEventModel(BaseBookingEventModel):
    id: int
    photos: List[str]


class ResponseGetBookingEventsModel(BaseModel):
    data: List[ResponseGetBookingEventModel]
