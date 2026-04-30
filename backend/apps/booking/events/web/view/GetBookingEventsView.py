from apps.booking.events.domain.models.SearchEventFiltersModel import (
    SearchEventFiltersModel,
)
from apps.booking.events.functions.core.IEventFilterConverterFunction import (
    IEventFilterConverterFunction,
)
from apps.booking.events.functions.core.IEventsGetFunction import IEventsGetFunction
from apps.booking.events.infrastructure.services.core.IBookingEventsService import (
    IBookingEventsService,
)
from fastapi import Depends
from functions.helpers import get_user_by_token


class GetBookingEventsView:

    def __init__(
        self,
        booking_events_service: IBookingEventsService,
        event_filter_converter: IEventFilterConverterFunction,
        event_get_function: IEventsGetFunction,
    ):
        self.__booking_events_service = booking_events_service
        self.__event_filter_converter = event_filter_converter
        self.__event_get_function = event_get_function

    async def __call__(self, token: str, filters: SearchEventFiltersModel = Depends()):
        user = await get_user_by_token(token)

        conditions, joins = await self.__event_filter_converter(
            event_filters=filters.filters
        )
        events = await self.__event_get_function(
            conditions=conditions, joins=joins, cashbox_id=user.cashbox_id
        )
        return events
