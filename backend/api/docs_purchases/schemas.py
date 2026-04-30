from typing import List, Optional

from pydantic import BaseModel


class Item(BaseModel):
    nomenclature: int
    price_type: Optional[int]
    price: float
    quantity: float
    unit: Optional[int]


class ItemGet(Item):
    nomenclature_name: Optional[str]
    unit_name: Optional[str]


class Create(BaseModel):
    number: Optional[str]
    dated: Optional[int]
    operation: Optional[str]
    comment: Optional[str]
    client: Optional[int]
    contragent: Optional[int]
    contract: Optional[int]
    status: Optional[bool]
    organization: int
    warehouse: Optional[int]
    purchased_by: Optional[int]
    docs_sales_id: Optional[int]
    goods: Optional[List[Item]]

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
    number: Optional[str]
    dated: Optional[int]
    status: bool
    operation: Optional[str]
    comment: Optional[str]
    client: Optional[int]
    contragent: Optional[int]
    contract: Optional[int]
    organization: int
    warehouse: Optional[int]
    sum: Optional[float]
    purchased_by: Optional[int]
    docs_sales_id: Optional[int]
    updated_at: int
    created_at: int


class ViewResult(BaseModel):
    result: List[ViewInList]
    count: int


class View(ViewInList):
    goods: Optional[List[ItemGet]]

    class Config:
        orm_mode = True


class ListView(BaseModel):
    __root__: Optional[List[ViewInList]]

    class Config:
        orm_mode = True
