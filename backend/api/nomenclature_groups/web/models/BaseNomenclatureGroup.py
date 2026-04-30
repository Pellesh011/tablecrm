from pydantic import BaseModel


class BaseNomenclatureGroup(BaseModel):
    name: str
