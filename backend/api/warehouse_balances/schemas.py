from typing import List, Optional

from pydantic import BaseModel


class View(BaseModel):
    id: int
    organization_id: Optional[int]
    warehouse_id: Optional[int]
    incoming_amount: Optional[int]
    outgoing_amount: Optional[int]
    current_amount: float
    cashbox_id: Optional[int]
    updated_at: int
    created_at: int


class ViewAlt(BaseModel):
    id: int
    name: str
    category: Optional[int]
    organization_id: Optional[int]
    organization_name: Optional[str]
    warehouse_id: Optional[int]
    warehouse_name: Optional[str]
    current_amount: float
    plus_amount: float
    minus_amount: float
    start_ost: float
    now_ost: float


class ViewAltList(BaseModel):
    name: str
    key: int
    children: List[ViewAlt]


class ViewRes(BaseModel):
    result: List[ViewAltList]


class ListView(BaseModel):
    __root__: Optional[List[View]]

    class Config:
        orm_mode = True


class RegisterStockView(BaseModel):
    id: int
    organization_id: Optional[int]
    warehouse_id: Optional[int]
    nomenclature_id: Optional[int]
    incoming_amount: Optional[int]
    outgoing_amount: Optional[int]
    current_amount: float
    cashbox_id: Optional[int]
    updated_at: int
    created_at: int


class WarehouseWithNomenclature(BaseModel):
    warehouse_name: Optional[str]
    current_amount: float
