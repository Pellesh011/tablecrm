import time
from typing import List

from apps.booking.booking.infrastructure.repositories.core.IBookingRepository import (
    IBookingRepository,
)
from apps.booking.events.domain.models.CreateBookingEventModel import (
    CreateBookingEventModel,
)
from apps.booking.events.infrastructure.services.core.IBookingEventsService import (
    IBookingEventsService,
)
from database.db import BookingEventStatus
from functions.helpers import get_user_by_token


class CreateBookingEventsView:

    def __init__(
        self,
        booking_events_service: IBookingEventsService,
        booking_repository: IBookingRepository,
    ):
        self.__booking_events_service = booking_events_service
        self.__booking_repository = booking_repository

    async def __call__(self, token: str, create_events: List[CreateBookingEventModel]):
        user = await get_user_by_token(token)

        created_events = await self.__booking_events_service.add_more(
            events=create_events, cashbox_id=user.cashbox_id
        )

        for event_booking in create_events:
            if event_booking.type == BookingEventStatus.give:
                booking_info = (
                    await self.__booking_repository.get_nearest_time_by_status(
                        current_date=int(time.time()),
                        booking_nomenclature_id=event_booking.booking_nomenclature_id,
                        cashbox_id=user.cashbox_id,
                        status="Новый",
                    )
                )
                if booking_info:
                    await self.__booking_repository.update_status(
                        booking_id=booking_info.id,
                        status="Забран",
                        cashbox_id=user.cashbox_id,
                    )

                    previous_booking_id = await self.__booking_repository.get_previous_by_date(
                        current_start=booking_info.start_booking,
                        cashbox_id=user.cashbox_id,
                        booking_nomenclature_id=event_booking.booking_nomenclature_id,
                        status="Доставлен",
                    )

                    if previous_booking_id:
                        await self.__booking_repository.update_status(
                            booking_id=previous_booking_id,
                            status="Завершен",
                            cashbox_id=user.cashbox_id,
                        )

            elif event_booking.type == BookingEventStatus.take:
                booking_info = (
                    await self.__booking_repository.get_nearest_time_by_status(
                        current_date=int(time.time()),
                        booking_nomenclature_id=event_booking.booking_nomenclature_id,
                        cashbox_id=user.cashbox_id,
                        status="Забран",
                    )
                )

                if booking_info:
                    await self.__booking_repository.update_status(
                        booking_id=booking_info.id,
                        status="Доставлен",
                        cashbox_id=user.cashbox_id,
                    )

        return created_events
