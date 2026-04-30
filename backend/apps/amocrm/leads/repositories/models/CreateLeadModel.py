from typing import Any, List, Optional

from pydantic import Field
from pydantic.main import BaseModel


class CustomFieldValueElement(BaseModel):
    value: Any


class CustomFieldValue(BaseModel):
    field_code: str
    values: List[CustomFieldValueElement]


class EmveddedContactModel(BaseModel):
    id: int


class EmveddedModel(BaseModel):
    contacts: Optional[List[EmveddedContactModel]]


class CreateLeadModel(BaseModel):
    name: str
    price: Optional[int]
    status_id: int
    custom_fields_values: Optional[List[CustomFieldValue]]
    embedded: Optional[EmveddedModel] = Field(None, alias="_embedded")
