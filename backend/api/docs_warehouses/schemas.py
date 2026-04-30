from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class WarehouseOperations(str, Enum):
    internal_consumption = "Внутреннее потребление"
    surplus_posting = "Оприходование излишков"
    movement = "Перемещение"
    write_off = "Списание"


class Item(BaseModel):
    price_type: Optional[int]
    price: float
    quantity: float
    unit: Optional[int]
    nomenclature: int


class ItemGood(BaseModel):
    price_type: Optional[int]
    price: float
    quantity: float
    unit: Optional[int]
    unit_name: Optional[str]
    tax: Optional[float]
    discount: Optional[float]
    sum_discounted: Optional[float]
    status: Optional[str]
    nomenclature: int
    nomenclature_name: Optional[str]


class ItemGet(Item):
    nomenclature_name: Optional[str]
    unit_name: Optional[str]


class Create(BaseModel):
    number: Optional[str]
    dated: Optional[int]
    contragent: Optional[int]
    docs_purchases: Optional[int] = None
    operation: Optional[str]
    to_warehouse: Optional[int]
    status: Optional[bool]
    comment: Optional[str]
    warehouse: Optional[int]
    organization: Optional[int]
    goods: Optional[List[Item]]
    status: Optional[bool] = True
    docs_sales_id: Optional[int] = None

    class Config:
        orm_mode = True


class Edit(Create):
    id: int

    class Config:
        orm_mode = True


class EditMass(BaseModel):
    __root__: List[Edit]

    class Config:
        orm_mode = True


class CreateMass(BaseModel):
    __root__: List[Create]

    class Config:
        orm_mode = True


class ViewInList(BaseModel):
    id: int
    tags: Optional[str]
    number: Optional[str]
    dated: Optional[int]
    contragent: Optional[int]
    operation: Optional[str]
    comment: Optional[str]
    organization: int
    status: Optional[bool]
    warehouse: Optional[int]
    to_warehouse: Optional[int]
    sum: Optional[float]
    updated_at: int
    created_at: int


class ViewForGoods(ViewInList):
    goods: Optional[List[ItemGood]]

    class Config:
        orm_mode = True


class GetDocsWarehouse(BaseModel):
    result: List[ViewForGoods]
    count: int


class View(ViewInList):
    goods: Optional[List[ItemGet]]

    class Config:
        orm_mode = True


class ListView(BaseModel):
    __root__: Optional[List[ViewInList]]

    class Config:
        orm_mode = True


class DeleteListView(ViewInList):
    deleted: Optional[List]
