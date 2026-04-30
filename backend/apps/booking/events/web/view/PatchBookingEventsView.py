from typing import List

from apps.booking.events.domain.models.PatchBookingEventsModel import (
    PatchBookingEventsModel,
)
from apps.booking.events.infrastructure.services.core.IBookingEventsService import (
    IBookingEventsService,
)
from functions.helpers import get_user_by_token


class PatchBookingEventsView:

    def __init__(
        self,
        booking_events_service: IBookingEventsService,
    ):
        self.__booking_events_service = booking_events_service

    async def __call__(self, token: str, patch_events: List[PatchBookingEventsModel]):
        user = await get_user_by_token(token)

        result = await self.__booking_events_service.patch_mass(
            patch_events=patch_events, cashbox_id=user.cashbox_id
        )

        return result
