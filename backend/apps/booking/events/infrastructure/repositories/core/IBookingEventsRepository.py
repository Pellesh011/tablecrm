from typing import Any, List, Mapping, Sequence

from ....domain.models.CreateBookingEventModel import CreateBookingEventModel
from ....domain.models.PatchBookingEventsModel import PatchBookingEventsModel


class IBookingEventsRepository:

    async def get_by_ids(self, event_ids: List[int], cashbox_id: int):
        raise NotImplementedError()

    async def get_by_nomenclature_ids(
        self, nomenclature_ids: Sequence[int], cashbox_id: int
    ):
        raise NotImplementedError()

    async def get_all(self, cashbox_id: int):
        raise NotImplementedError()

    async def add_one(self, event: CreateBookingEventModel) -> int:
        raise NotImplementedError()

    async def add_more(self, events: List[Mapping[str, Any]]):
        raise NotImplementedError()

    async def add_photos(self, events_photos: List[Mapping[str, Any]]):
        raise NotImplementedError()

    async def delete_photos_by_event_ids(self, event_ids: List[int]):
        raise NotImplementedError()

    async def delete_photos_by_ids(self, photo_ids: List[int]):
        raise NotImplementedError()

    async def delete_by_ids(self, event_ids: List[int], cashbox_id: int):
        raise NotImplementedError()

    async def patch(self, patch_event: PatchBookingEventsModel, cashbox_id: int):
        raise NotImplementedError()
