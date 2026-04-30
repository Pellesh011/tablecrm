from typing import List, Optional

from pydantic import BaseModel


class Cheque(BaseModel):
    id: int
    user: Optional[int]
    created_at: int
    data: dict


class Cheques(BaseModel):
    __root__: Optional[list[Cheque]]


class ChequesGet(BaseModel):
    result: Optional[List[Cheque]]
    count: int
