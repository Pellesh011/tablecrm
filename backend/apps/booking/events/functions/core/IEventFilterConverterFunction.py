from ...domain.models.BaseSearchEventFiltersModel import BaseSearchEventFiltersModel


class IEventFilterConverterFunction:

    async def __call__(self, event_filters: BaseSearchEventFiltersModel):
        raise NotImplementedError()
