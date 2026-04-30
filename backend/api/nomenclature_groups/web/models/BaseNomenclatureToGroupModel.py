from pydantic import BaseModel


class BaseNomenclatureToGroupModel(BaseModel):
    nomenclature_id: int
    group_id: int
