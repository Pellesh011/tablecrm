from typing import List, Optional

from pydantic import BaseModel


class Function(BaseModel):
    entity_or_function: str
    status: bool = True

    class Config:
        orm_mode = True


class FunctionList(BaseModel):
    __root__: Optional[List[Function]]

    class Config:
        orm_mode = True


class FunctionListGet(BaseModel):
    result: Optional[List[Function]]
    count: int
