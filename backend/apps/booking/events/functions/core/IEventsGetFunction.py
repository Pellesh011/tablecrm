from apps.booking.events.domain.models.ResponseGetBookingEventModel import (
    ResponseGetBookingEventsModel,
)


class IEventsGetFunction:

    async def __call__(
        self,
        conditions: list,
        joins: list,
        cashbox_id: int,
        page: int = 1,
        size: int = 50,
    ) -> ResponseGetBookingEventsModel:
        raise NotImplementedError()
