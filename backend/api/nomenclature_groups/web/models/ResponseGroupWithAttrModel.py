from typing import List

from pydantic import BaseModel


class NomenclatureAttrValueModel(BaseModel):
    attribute_value_id: int
    value: str


class NomenclatureAttrModel(BaseModel):
    attribute_id: int
    attribute_name: str
    attribute_alias: str
    attribute_value: NomenclatureAttrValueModel


class NomenclatureWithAttrModel(BaseModel):
    nomenclature_id: int
    attribute: NomenclatureAttrModel


class ResponseGroupWithAttrModel(BaseModel):
    group_id: int
    nomenclatures: List[NomenclatureWithAttrModel]
