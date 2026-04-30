from enum import Enum
from typing import List, Optional

from database.enums import DebitCreditType
from pydantic import BaseModel


class ExpensesFor(str, Enum):
    buying = "При приобретении"
    other = "В иных процессах"


class DistributeFor(str, Enum):
    prime_cost = "На себестоимость товаров"
    current_costs = "На расходы текущего периода"
    future_costs = "На расходы будущего периода"
    manual = "Распредилить вручную"


class DistributeAccording(str, Enum):
    prime_cost = "Себестоимость"
    costs = "Расходы"


class Article(BaseModel):
    id: int
    name: str
    emoji: Optional[str]
    icon_file: Optional[str]
    code: Optional[int]
    description: Optional[str]
    expenses_for: Optional[str]
    distribute_for: Optional[str]
    distribute_according: Optional[str]
    created_at: int
    updated_at: int
    dc: Optional[DebitCreditType]

    class Config:
        orm_mode = True


class ArticleEdit(BaseModel):
    id: int
    name: Optional[str]
    emoji: Optional[str]
    icon_file: Optional[str]
    code: Optional[int]
    description: Optional[str]
    expenses_for: Optional[ExpensesFor]
    distribute_for: Optional[DistributeFor]
    distribute_according: Optional[DistributeAccording]
    dc: Optional[DebitCreditType]

    class Config:
        orm_mode = True


class ArticleCreate(BaseModel):
    name: str
    emoji: Optional[str]
    code: Optional[int]
    description: Optional[str]
    expenses_for: Optional[ExpensesFor]
    distribute_for: Optional[DistributeFor]
    distribute_according: Optional[DistributeAccording]
    dc: Optional[DebitCreditType]

    class Config:
        orm_mode = True


class GetArticles(BaseModel):
    result: Optional[List[Article]]
    count: int

    class Config:
        orm_mode = True
