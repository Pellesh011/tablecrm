from typing import List, Optional

from api.nomenclature_groups.infrastructure.models.NomenclatureGroupModel import (
    NomenclatureGroupModel,
)
from pydantic import BaseModel


class Nomenclature(BaseModel):
    id: int
    name: Optional[str]
    is_main: bool


class GroupModelWithNomenclaturesModel(NomenclatureGroupModel):
    nomenclatures: Optional[List[Nomenclature]]
