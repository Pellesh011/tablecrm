from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from database.db import (
    booking,
    booking_events,
    booking_events_photo,
    booking_nomenclature,
    database,
    pictures,
)
from sqlalchemy import select

from ...domain.models.ResponseGetBookingEventModel import ResponseGetBookingEventsModel
from ..core.IEventsGetFunction import IEventsGetFunction


class EventsGetFunction(IEventsGetFunction):

    def __init__(self, s3_factory: IS3ServiceFactory):
        self.__s3_factory = s3_factory

    async def __call__(
        self,
        conditions: list,
        joins: list,
        cashbox_id: int,
        page: int = 1,
        size: int = 50,
    ) -> ResponseGetBookingEventsModel:
        s3_client = self.__s3_factory()

        query = (
            select(
                booking_events.c.id,
                booking_events.c.booking_nomenclature_id,
                booking_events.c.type,
                booking_events.c.value,
                booking_events.c.latitude,
                booking_events.c.longitude,
                booking_events.c.created_at,
                booking_events.c.updated_at,
                booking_events.c.is_deleted,
            )
            .join(
                booking_nomenclature,
                booking_events.c.booking_nomenclature_id == booking_nomenclature.c.id,
            )
            .join(booking, booking_nomenclature.c.booking_id == booking.c.id)
            .where(booking.c.cashbox == cashbox_id)
        )
        for join_model in joins:
            query = query.join(*join_model)

        for condition in conditions:
            query = query.filter(condition)

        events = await database.fetch_all(query)

        return_dict = []

        for event in events:
            event_dict = dict(event)
            query = (
                select(pictures.c.url)
                .join(pictures, pictures.c.id == booking_events_photo.c.photo_id)
                .where(booking_events_photo.c.booking_event_id == event.id)
            )

            pictures_list = await database.fetch_all(query)

            event_dict["photos"] = []

            for picture_info in pictures_list:

                url = await s3_client.get_link_object(
                    bucket_name="5075293c-docs_generated", file_key=picture_info.url
                )
                event_dict["photos"].append(url)

            return_dict.append(event_dict)

        return ResponseGetBookingEventsModel(data=return_dict)
