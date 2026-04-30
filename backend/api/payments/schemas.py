from enum import Enum
from typing import List, Optional

from functions.helpers import sanitize_float
from pydantic import BaseModel, validator


class RepeatPeriod(str, Enum):
    YEARLY = "yearly"
    MONTHLY = "monthly"
    WEEKLY = "weekly"
    DAILY = "daily"
    HOURLY = "hourly"
    SECONDS = "seconds"


class PaymentType(str, Enum):
    incoming = "incoming"
    outgoing = "outgoing"
    transfer = "transfer"


class PaymentRepeat(BaseModel):
    repeat_parent_id: Optional[int]
    repeat_period: Optional[RepeatPeriod]
    repeat_weekday: Optional[str]
    repeat_day: Optional[int]
    repeat_month: Optional[int]
    repeat_first: Optional[int]
    repeat_last: Optional[int]
    repeat_seconds: Optional[int]
    repeat_number: Optional[int]


class PaymentCreate(BaseModel):
    contragent: Optional[int]
    type: PaymentType
    name: Optional[str]
    external_id: Optional[str]
    tags: Optional[str] = ""
    amount_without_tax: float
    article: Optional[str]
    article_id: Optional[int]
    project_id: Optional[int]
    amount: float
    description: Optional[str]
    paybox: int
    paybox_to: Optional[int]
    date: Optional[int]
    repeat_freq: Optional[int]
    repeat: Optional[PaymentRepeat]
    status: bool
    stopped: bool
    tax: Optional[int]
    tax_type: Optional[str]
    deb_cred: Optional[bool]
    cheque: Optional[int]
    docs_sales_id: Optional[int]
    docs_purchases_id: Optional[int]
    contract_id: Optional[int]

    preamount: Optional[float]
    percentamount: Optional[float]

    @validator("amount")
    def validate_amount(cls, value):
        return sanitize_float(value)

    @validator("amount_without_tax")
    def validate_amount_without_tax(cls, value):
        return sanitize_float(value)

    @validator("preamount")
    def validate_preamount(cls, value):
        return sanitize_float(value)

    @validator("percentamount")
    def validate_percentamount(cls, value):
        return sanitize_float(value)

    class Config:
        orm_mode = True


class PaymentGet(BaseModel):
    contragent: Optional[int]
    type: str
    name: str
    external_id: Optional[str]
    tags: Optional[str]
    external_id: Optional[str]
    amount_without_tax: float
    article: Optional[str]
    article_id: Optional[int]
    project_id: Optional[int]
    amount: float
    description: Optional[str]
    paybox: int
    paybox_to: Optional[int]
    date: Optional[int]
    repeat_freq: Optional[int]
    repeat: Optional[PaymentRepeat]
    status: bool
    stopped: bool
    tax: Optional[int]
    tax_type: Optional[str]
    deb_cred: Optional[bool]
    cheque: Optional[int]
    docs_sales_id: Optional[int]
    docs_purchases_id: Optional[int]

    @validator("amount")
    def validate_amount(cls, value):
        return sanitize_float(value)

    @validator("amount_without_tax")
    def validate_amount_without_tax(cls, value):
        return sanitize_float(value)

    class Config:
        orm_mode = True


class PaymentDB(PaymentCreate):
    account: int
    cashbox: int
    paybox: int
    created_at: int
    updated_at: int

    class Config:
        orm_mode = True


class PaymentInList(BaseModel):
    id: int
    contragent: Optional[int]
    type: str
    name: Optional[str]
    external_id: Optional[str]
    tags: Optional[str]
    amount_without_tax: Optional[float]
    article: Optional[str]
    article_id: Optional[int]
    project_id: Optional[int]
    amount: Optional[float]
    description: Optional[str]
    paybox: Optional[int] = None
    paybox_to: Optional[int]
    source_account_name: Optional[str]
    source_account_id: Optional[int]
    date: Optional[int]
    repeat_freq: Optional[int]
    repeat: Optional[PaymentRepeat]
    status: bool
    stopped: bool
    tax: Optional[int]
    tax_type: Optional[str]
    deb_cred: Optional[bool]
    stopped: bool
    raspilen: Optional[bool]
    parent_id: Optional[int]
    contragent_name: Optional[str]
    cheque: Optional[int]
    docs_sales_id: Optional[int]
    docs_purchases_id: Optional[int]
    created_at: int
    updated_at: int
    can_be_deleted_or_edited: Optional[bool] = True

    @validator("amount")
    def validate_amount(cls, value):
        return sanitize_float(value)

    @validator("amount_without_tax")
    def validate_amount_without_tax(cls, value):
        return sanitize_float(value)

    class Config:
        orm_mode = True


class PaymentInListWithData(PaymentInList):
    data: dict


class PaymentEdit(BaseModel):
    type: Optional[PaymentType]
    name: Optional[str]
    tags: Optional[str]
    external_id: Optional[str]
    article: Optional[str]
    article_id: Optional[int]
    project_id: Optional[int]
    description: Optional[str]
    repeat_freq: Optional[int]
    repeat: Optional[PaymentRepeat]
    tax_type: Optional[str]
    paybox: Optional[int]
    paybox_to: Optional[int]
    contragent: Optional[int]
    amount: Optional[float]
    amount_without_tax: Optional[float]
    date: Optional[int]
    status: Optional[bool]
    stopped: Optional[bool]
    tax: Optional[int]
    deb_cred: Optional[bool]
    cheque: Optional[int]
    docs_sales_id: Optional[int]
    docs_purchases_id: Optional[int]

    @validator("amount")
    def validate_amount(cls, value):
        return sanitize_float(value)

    @validator("amount_without_tax")
    def validate_amount_without_tax(cls, value):
        return sanitize_float(value)

    class Config:
        orm_mode = True


class ChildrenEdit(BaseModel):
    id: int
    article_id: Optional[int]
    project_id: Optional[int]
    contragent: Optional[int]
    amount: Optional[float]

    class Config:
        orm_mode = True


class PaymentDelete(BaseModel):
    id: int

    class Config:
        orm_mode = True


class GetPayments(BaseModel):
    result: Optional[List[PaymentInList]]
    count: int

    class Config:
        orm_mode = True


class GetPaymentsBasic(BaseModel):
    result: Optional[List[PaymentInList]]
    count: int
    errors: Optional[dict] = None


class PaymentMeta(BaseModel):
    name: str
    tags: str

    class Config:
        orm_mode = True


class GetPaymentsMeta(BaseModel):
    result: Optional[List[PaymentMeta]]

    class Config:
        orm_mode = True


class PaymentChildren(BaseModel):
    amount: float
    project: Optional[int]
    ca: Optional[int]

    class Config:
        orm_mode = True
