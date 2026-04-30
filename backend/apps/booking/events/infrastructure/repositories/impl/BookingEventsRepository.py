from typing import Any, List, Mapping

from apps.booking.events.domain.models.CreateBookingEventModel import (
    CreateBookingEventModel,
)
from apps.booking.events.domain.models.PatchBookingEventsModel import (
    PatchBookingEventsModel,
)
from database.db import (
    booking,
    booking_events,
    booking_events_photo,
    booking_nomenclature,
    database,
)
from sqlalchemy import and_, select

from ..core.IBookingEventsRepository import IBookingEventsRepository


class BookingEventsRepository(IBookingEventsRepository):

    async def get_by_ids(self, event_ids: List[int], cashbox_id: int):
        query = (
            select(booking_events.c.id)
            .join(
                booking_nomenclature,
                booking_events.c.booking_nomenclature_id == booking_nomenclature.c.id,
            )
            .join(booking, booking_nomenclature.c.booking_id == booking.c.id)
            .where(
                and_(
                    booking_events.c.id.in_(event_ids), booking.c.cashbox == cashbox_id
                )
            )
        )
        event_ids = await database.fetch_all(query)
        return [element.id for element in event_ids]

    async def get_by_nomenclature_ids(self, nomenclature_id: int, cashbox_id: int):
        pass

    async def add_one(self, event: CreateBookingEventModel) -> int:
        query = (
            booking_events.insert()
            .values(
                booking_nomenclature_id=event.booking_nomenclature_id,
                type=event.type,
                value=event.value,
                latitude=event.latitude,
                longitude=event.longitude,
                is_deleted=False,
            )
            .returning(booking_events.c.id)
        )
        result = await database.fetch_one(query)
        return result.id

    async def add_more(self, events: List[Mapping[str, Any]]) -> List[int]:
        query = booking_events.insert().values(events).returning(booking_events.c.id)
        result = await database.fetch_all(query)
        return [element.id for element in result]

    async def add_photos(self, events_photos: List[Mapping[str, Any]]):
        query = booking_events_photo.insert().values(events_photos)
        await database.execute(query)

    async def delete_photos_by_event_ids(self, event_ids: List[int]):
        query = booking_events_photo.delete().where(
            booking_events_photo.c.booking_event_id.in_(event_ids)
        )
        await database.execute(query)

    async def delete_photos_by_ids(self, photo_ids: List[int]):
        query = booking_events_photo.delete().where(
            booking_events_photo.c.id.in_(photo_ids)
        )
        await database.execute(query)

    async def delete_by_ids(self, event_ids: List[int], cashbox_id: int):
        query = booking_events.delete().where(booking_events.c.id.in_(event_ids))
        await database.execute(query)

    async def patch(self, patch_event: PatchBookingEventsModel, cashbox_id: int):
        query = (
            booking.select()
            .join(
                booking_nomenclature, booking.c.id == booking_nomenclature.c.booking_id
            )
            .join(
                booking_events,
                booking_nomenclature.c.id == booking_events.c.booking_nomenclature_id,
            )
            .where(
                and_(
                    booking.c.cashbox == cashbox_id,
                    booking_events.c.id == patch_event.id,
                )
            )
        )
        booking_info = await database.fetch_one(query)

        if not booking_info:
            return

        values = {}
        if patch_event.value:
            values["value"] = patch_event.value
        if patch_event.type:
            values["type"] = patch_event.type
        if patch_event.latitude:
            values["latitude"] = patch_event.latitude
        if patch_event.longitude:
            values["longitude"] = patch_event.longitude
        if values:
            query = (
                booking_events.update()
                .where(booking_events.c.id == patch_event.id)
                .values(values)
            )
            await database.execute(query)
            values["id"] = patch_event.id
            return values
