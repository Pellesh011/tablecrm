from typing import List

from apps.booking.events.domain.models.AddBookingEventPhotoModel import (
    AddBookingEventPhotoModel,
)
from apps.booking.events.infrastructure.services.core.IBookingEventsService import (
    IBookingEventsService,
)
from functions.helpers import get_user_by_token


class AddImageBookingEventView:

    def __init__(self, booking_events_service: IBookingEventsService):
        self.__booking_events_service = booking_events_service

    async def __call__(self, token: str, add_photos: List[AddBookingEventPhotoModel]):
        user = await get_user_by_token(token)

        await self.__booking_events_service.add_photos(
            events_photo=add_photos, cashbox_id=user.cashbox_id
        )
