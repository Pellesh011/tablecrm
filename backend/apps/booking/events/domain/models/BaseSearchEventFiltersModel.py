from typing import List

from pydantic import BaseModel


class BaseSearchEventFiltersModel(BaseModel):
    nomenclature_ids: List[int]
