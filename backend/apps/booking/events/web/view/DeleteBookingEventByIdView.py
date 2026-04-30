from typing import List

from apps.booking.events.infrastructure.services.core.IBookingEventsService import (
    IBookingEventsService,
)
from fastapi import Body
from functions.helpers import get_user_by_token


class DeleteBookingEventByIdView:

    def __init__(self, booking_events_service: IBookingEventsService):
        self.__booking_events_service = booking_events_service

    async def __call__(
        self, token: str, event_ids: List[int] = Body(..., example=[1, 2, 3])
    ):
        user = await get_user_by_token(token)

        await self.__booking_events_service.delete_by_ids(
            event_ids=event_ids, cashbox_id=user.cashbox_id
        )
