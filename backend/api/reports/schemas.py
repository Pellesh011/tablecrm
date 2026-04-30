from typing import Optional

from pydantic import BaseModel


class ReportData(BaseModel):
    paybox: Optional[list[int]]
    datefrom: Optional[int]
    dateto: Optional[int]
    user: Optional[int] = None
