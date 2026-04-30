from pydantic import BaseModel


class ResponsePatchNomenclatureGroupModel(BaseModel):
    id: int
    cashbox_id: int
    name: str
