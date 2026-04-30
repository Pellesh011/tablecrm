from typing import Optional

from database.enums import DebitCreditType
from pydantic import BaseModel


class PaymentFiltersQuery(BaseModel):
    name: Optional[str]
    tags: Optional[str]
    external_id: Optional[str]
    relship: Optional[str]
    project: Optional[str]
    contragent: Optional[str]
    paybox: Optional[str]
    paybox_to: Optional[str]
    source_account: Optional[str]
    dateto: Optional[str]
    datefrom: Optional[str]
    payment_type: Optional[str]
    include_paybox_dest: Optional[bool] = False
    timezone: Optional[str] = "UTC"


class AnalyticsFiltersQuery(BaseModel):
    datefrom: Optional[int]
    dateto: Optional[int]
    paybox_id: Optional[str]
    status: Optional[str]


class ChequesFiltersQuery(BaseModel):
    datefrom: Optional[int]
    dateto: Optional[int]
    user: Optional[int]


class PayboxesFiltersQuery(BaseModel):
    external_id: Optional[str]
    name: Optional[str]


class ProjectsFiltersQuery(BaseModel):
    external_id: Optional[str]
    name: Optional[str]


class ArticlesFiltersQuery(BaseModel):
    name: Optional[str]
    dc: Optional[DebitCreditType]


class UsersFiltersQuery(BaseModel):
    external_id: Optional[str]


class CAFiltersQuery(BaseModel):
    name: Optional[str]
    inn: Optional[int]
    phone: Optional[str]
    external_id: Optional[str]


class PicturesFiltersQuery(BaseModel):
    entity: Optional[str]
    entity_id: Optional[int]


class PricesFiltersQuery(BaseModel):
    name: Optional[str]
    type: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[int]
    unit: Optional[int]
    category_ids: Optional[str]
    manufacturer: Optional[int]
    price_type_id: Optional[int]
    date_from: Optional[int]
    date_to: Optional[int]
    price_type_tags: Optional[str] = None
    price_type_tags_mode: Optional[str] = "or"


class CUIntegerFilters(BaseModel):
    updated_at__gte: Optional[int] = None
    updated_at__lte: Optional[int] = None
    created_at__gte: Optional[int] = None
    created_at__lte: Optional[int] = None
