from typing import List, Optional

from api.pictures.schemas import Picture
from api.price_types.schemas import PriceType
from api.warehouses.schemas import Warehouse
from pydantic import BaseModel


class PriceInList(BaseModel):
    id: int
    unit_name: Optional[str]
    category_name: Optional[str]
    manufacturer_name: Optional[str]
    price: float
    date_from: Optional[int]
    date_to: Optional[int]
    price_types: Optional[List[PriceType]]

    class Config:
        orm_mode = True


class ViewAlt(BaseModel):
    organization_id: Optional[int]
    organization_name: Optional[str]
    current_amount: float
    plus_amount: float
    minus_amount: float
    start_ost: float
    now_ost: float
    warehouses: Optional[List[Warehouse]]


class ViewAltList(BaseModel):
    name: str
    key: int
    children: List[ViewAlt]


class WebappItem(BaseModel):
    id: int
    name: str
    type: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[int]
    unit: Optional[int]
    category: Optional[int]
    manufacturer: Optional[int]
    updated_at: int
    created_at: int
    unit_name: Optional[str]
    pictures: Optional[List[Picture]]
    prices: Optional[List[PriceInList]]
    alt_warehouse_balances: Optional[List[ViewAltList]]


class WebappResponse(BaseModel):
    result: Optional[List[WebappItem]]
    count: int
