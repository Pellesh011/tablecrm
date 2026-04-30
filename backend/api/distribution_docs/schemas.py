from typing import List, Optional

from pydantic import BaseModel


class Item(BaseModel):
    document_sale: Optional[int]
    document_purchase: Optional[int]
    document_warehouse: Optional[int]
    dated: int
    nomenclature: int
    start_amount: int
    start_price: float
    incoming_amount: Optional[int]
    incoming_price: Optional[float]
    outgoing_amount: Optional[int]
    outgoing_price: Optional[float]
    end_amount: int
    end_price: float


class ViewInList(BaseModel):
    id: int
    organization: int
    period_start: int
    period_end: int
    updated_at: int
    created_at: int


class View(ViewInList):
    table: Optional[List[Item]]

    class Config:
        orm_mode = True


class ListView(BaseModel):
    __root__: Optional[List[ViewInList]]

    class Config:
        orm_mode = True


class ListViewGet(BaseModel):
    result: Optional[List[ViewInList]]
    count: int
