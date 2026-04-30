from database.db import booking_events

from ....events.functions.core.IEventFilterConverterFunction import (
    IEventFilterConverterFunction,
)
from ...domain.models.SearchEventFiltersModel import BaseSearchEventFiltersModel


class EventFilterConverterFunction(IEventFilterConverterFunction):

    async def __call__(self, event_filters: BaseSearchEventFiltersModel):
        additions = []
        joins = []

        if event_filters.nomenclature_ids:
            additions.append(
                booking_events.c.booking_nomenclature_id.in_(
                    event_filters.nomenclature_ids
                )
            )
        return additions, joins
