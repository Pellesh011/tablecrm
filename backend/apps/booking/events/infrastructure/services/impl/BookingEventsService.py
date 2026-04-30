import asyncio
from typing import List

from apps.booking.nomenclature.infrastructure.repositories.core.IBookingNomenclatureRepository import (
    IBookingNomenclatureRepository,
)
from database.db import (
    booking_events_photo,
    database,
    pictures,
)
from fastapi import HTTPException
from sqlalchemy import and_, select
from starlette import status

from ....domain.models.AddBookingEventPhotoModel import AddBookingEventPhotoModel
from ....domain.models.CreateBookingEventModel import CreateBookingEventModel
from ....domain.models.PatchBookingEventsModel import PatchBookingEventsModel
from ....domain.models.ReponseCreatedBookingEventModel import (
    ResponseCreatedBookingEventModel,
)
from ...repositories.core.IBookingEventsRepository import IBookingEventsRepository
from ..core.IBookingEventsService import IBookingEventsService


class BookingEventsService(IBookingEventsService):

    def __init__(
        self,
        booking_events_repository: IBookingEventsRepository,
        booking_nomenclature_repository: IBookingNomenclatureRepository,
    ):
        self.__booking_events_repository = booking_events_repository
        self.__booking_nomenclature_repository = booking_nomenclature_repository

    async def add_one(
        self, events: CreateBookingEventModel
    ) -> ResponseCreatedBookingEventModel:
        created_event_id = await self.__booking_events_repository.add_one(event=events)
        return ResponseCreatedBookingEventModel(id=created_event_id, **events.dict())

    async def add_more(self, events: List[CreateBookingEventModel], cashbox_id: int):
        async def convert_to_save(row: CreateBookingEventModel):
            return {
                "booking_nomenclature_id": await self.__booking_nomenclature_repository.get_by_id(
                    cashbox=cashbox_id, nomenclature_id=row.booking_nomenclature_id
                ),
                "type": row.type,
                "value": row.value,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "is_deleted": False,
            }

        tasks = [convert_to_save(row) for row in events]
        for_create_list = await asyncio.gather(*tasks)

        created_event_ids = await self.__booking_events_repository.add_more(
            events=for_create_list
        )
        return [
            {"id": created_event_id, **created_event}
            for created_event, created_event_id in zip(
                for_create_list, created_event_ids
            )
        ]

    async def add_photos(
        self, events_photo: List[AddBookingEventPhotoModel], cashbox_id: int
    ):

        for_create_list = []
        for event in events_photo:
            query = select(pictures.c.id).where(
                and_(pictures.c.cashbox == cashbox_id, pictures.c.id == event.photo_id)
            )
            result = await database.fetch_one(query)
            if result:
                for_create_list.append(
                    {"booking_event_id": event.event_id, "photo_id": event.photo_id}
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={"error": f"Picture with ID: {event.photo_id} not found"},
                )

        if for_create_list:
            await self.__booking_events_repository.add_photos(
                events_photos=for_create_list
            )

    async def delete_by_ids(self, event_ids: List[int], cashbox_id: int):
        deleted_ids = await self.__booking_events_repository.get_by_ids(
            event_ids=event_ids, cashbox_id=cashbox_id
        )

        await self.__booking_events_repository.delete_photos_by_event_ids(
            event_ids=deleted_ids
        )

        await self.__booking_events_repository.delete_by_ids(
            event_ids=deleted_ids, cashbox_id=cashbox_id
        )

    async def delete_photos_by_ids(self, photo_ids: List[int], cashbox_id: int):
        query = (
            select(booking_events_photo.c.id)
            .join(pictures, booking_events_photo.c.photo_id == pictures.c.id)
            .where(
                and_(
                    booking_events_photo.c.id.in_(photo_ids),
                    pictures.c.owner == cashbox_id,
                )
            )
        )
        photo_ids = await database.fetch_all(query)
        deleted_ids = [element.id for element in photo_ids]

        await self.__booking_events_repository.delete_photos_by_ids(
            photo_ids=deleted_ids
        )

    async def patch_mass(
        self, patch_events: List[PatchBookingEventsModel], cashbox_id: int
    ):
        return_updated = []
        for event in patch_events:
            result = await self.__booking_events_repository.patch(
                patch_event=event, cashbox_id=cashbox_id
            )
            if result:
                return_updated.append(result)

        return return_updated
