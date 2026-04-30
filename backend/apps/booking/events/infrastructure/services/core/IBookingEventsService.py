from typing import List

from ....domain.models.AddBookingEventPhotoModel import AddBookingEventPhotoModel
from ....domain.models.CreateBookingEventModel import CreateBookingEventModel
from ....domain.models.PatchBookingEventsModel import PatchBookingEventsModel
from ....domain.models.ReponseCreatedBookingEventModel import (
    ResponseCreatedBookingEventModel,
)


class IBookingEventsService:

    async def add_one(
        self, event: CreateBookingEventModel
    ) -> ResponseCreatedBookingEventModel:
        raise NotImplementedError()

    async def add_more(self, events: List[CreateBookingEventModel], cashbox_id: int):
        raise NotImplementedError()

    async def add_photos(
        self, events_photo: List[AddBookingEventPhotoModel], cashbox_id: int
    ):
        raise NotImplementedError()

    async def delete_by_ids(self, event_ids: List[int], cashbox_id: int):
        raise NotImplementedError()

    async def delete_photos_by_ids(self, photo_ids: List[int], cashbox_id: int):
        raise NotImplementedError()

    async def patch_mass(
        self, patch_events: List[PatchBookingEventsModel], cashbox_id: int
    ):
        raise NotImplementedError()
