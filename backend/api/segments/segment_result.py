from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class Contragent(BaseModel):
    id: int
    name: Optional[str]
    phone: Optional[str]


class SegmentContragentData(BaseModel):
    id: int
    updated_at: Optional[datetime] = None
    contragents: Optional[List[Contragent]] = []
    added_contragents: Optional[List[Contragent]] = []
    deleted_contragents: Optional[List[Contragent]] = []
