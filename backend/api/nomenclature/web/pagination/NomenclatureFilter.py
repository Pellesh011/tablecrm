from dataclasses import dataclass
from enum import Enum
from typing import Optional

from fastapi import Query


class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"


@dataclass
class NomenclatureFilter:
    order_created_at: Optional[SortOrder] = Query(None, alias="order[created_at]")
    order_price: Optional[SortOrder] = Query(None, alias="order[price]")
    order_name: Optional[SortOrder] = Query(None, alias="order[name]")
