from typing import List, Optional

from pydantic import BaseModel


class Create(BaseModel):
    contragent_id: Optional[int]
    organization_id: Optional[int]
    contragent_inn: Optional[int]
    organization_inn: Optional[int]
    period_from: Optional[int]
    period_to: Optional[int]
    contract_id: Optional[int]
    group_by_contract: Optional[bool]

    class Config:
        orm_mode = True


class CreateMass(BaseModel):
    __root__: List[Create]

    class Config:
        orm_mode = True


class CreateError(Create):
    error: str

    class Config:
        orm_mode = True


class CreateErrorMass(BaseModel):
    __root__: List[CreateError]

    class Config:
        orm_mode = True


class ViewInList(BaseModel):
    id: int
    number: Optional[str]
    dated: Optional[int]
    organization: int
    contragent: int
    contract: Optional[int]
    organization_name: str
    contragent_name: str
    period_from: Optional[int]
    period_to: Optional[int]
    organization_period_debt: Optional[float]
    organization_period_credit: Optional[float]
    contragent_period_debt: Optional[float]
    contragent_period_credit: Optional[float]
    organization_initial_balance: Optional[float]
    contragent_initial_balance: Optional[float]
    organization_closing_balance: Optional[float]
    contragent_closing_balance: Optional[float]
    updated_at: int
    created_at: int


class View(ViewInList):
    documents: Optional[List[dict]]
    documents_grouped: Optional[dict[Optional[str], list[dict]]]

    class Config:
        orm_mode = True


class ListView(BaseModel):
    __root__: Optional[List[ViewInList]]

    class Config:
        orm_mode = True


class ListViewGet(BaseModel):
    result: Optional[List[ViewInList]]
    count: int


class CreateListView(BaseModel):
    results: Optional[List[ViewInList]]
    errors: CreateErrorMass

    class Config:
        orm_mode = True
