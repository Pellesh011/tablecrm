from apps.booking.events.domain.models.BaseSearchEventFiltersModel import (
    BaseSearchEventFiltersModel,
)
from pydantic import BaseModel


class SearchEventFiltersModel(BaseModel):
    filters: BaseSearchEventFiltersModel
