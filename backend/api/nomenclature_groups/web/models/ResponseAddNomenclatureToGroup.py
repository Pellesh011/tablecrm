from pydantic import BaseModel


class ResponseAddNomenclatureToGroup(BaseModel):
    id: int
    group_id: int
    nomenclature_id: int
    is_main: bool
