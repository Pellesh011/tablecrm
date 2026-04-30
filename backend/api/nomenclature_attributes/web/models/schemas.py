from typing import List, Optional

from pydantic import BaseModel


class AttributeCreate(BaseModel):
    name: str
    alias: Optional[str] = None


class AttributeCreateResponse(BaseModel):
    id: int
    name: str
    alias: Optional[str] = None


class AttributeValue(BaseModel):
    attribute_id: int
    value: str


class AttributeValues(BaseModel):
    attribute_value_id: int
    value: str


class ResponseAttributeValue(BaseModel):
    attribute_id: int
    attribute_value: List[AttributeValues]


class AttributeValueCreate(BaseModel):
    nomenclature_id: int
    attributes: List[AttributeValue]


class AttributeValueResponse(BaseModel):
    nomenclature_id: int
    attributes: List[ResponseAttributeValue]


class AttributeResponse(BaseModel):
    id: int
    name: str
    alias: str
    values: str


class NomenclatureWithAttributesResponse(BaseModel):
    nomenclature_id: int
    attributes: List[AttributeResponse]


class NomenclatureAttribute(BaseModel):
    name: str
    value: str


class NomenclatureRelations(BaseModel):
    nomenclature_ids: List[int]

    class Config:
        schema_extra = {"example": {"nomenclature_ids": []}}


class AddNomenclatureRequest(BaseModel):
    group_id: int
    nomenclature_id: int


class AddNomenclatureResponse(BaseModel):
    message: str
    group_id: int
    nomenclature_id: int


class NomenclatureGroupResponse(BaseModel):
    nomenclature_id: int
    group_id: int
