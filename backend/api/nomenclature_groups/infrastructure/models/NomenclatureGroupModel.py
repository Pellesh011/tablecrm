from pydantic import BaseModel


class NomenclatureGroupModel(BaseModel):
    id: int
    name: str
    cashbox_id: int
