from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel


class TypeDoc(str, Enum):
    html = "html"
    pdf = "pdf"


class VariableType(BaseModel):
    result: Optional[Dict]

    class Config:
        orm_mode = True


class Generate(BaseModel):
    template_id: int
    variable: Dict
    type_doc: TypeDoc
    entity: str = None
    entity_id: int = None
    tags: str = None


class ReGenerateList(BaseModel):
    __root__: Optional[List[Generate]]
