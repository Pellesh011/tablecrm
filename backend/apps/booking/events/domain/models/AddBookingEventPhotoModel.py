from pydantic import BaseModel


class AddBookingEventPhotoModel(BaseModel):
    event_id: int
    photo_id: int
