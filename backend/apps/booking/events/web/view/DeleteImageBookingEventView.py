from typing import List

from apps.booking.events.infrastructure.services.core.IBookingEventsService import (
    IBookingEventsService,
)
from functions.helpers import get_user_by_token


class DeleteImageBookingEventView:

    def __init__(self, booking_events_service: IBookingEventsService):
        self.__booking_events_service = booking_events_service

    async def __call__(self, token: str, delete_photos: List[int]):
        user = await get_user_by_token(token)

        await self.__booking_events_service.delete_photos_by_ids(
            photo_ids=delete_photos, cashbox_id=user.cashbox_id
        )
