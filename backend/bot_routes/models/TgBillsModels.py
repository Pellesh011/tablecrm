from datetime import datetime
from typing import Optional

from database.db import TgBillStatus
from pydantic import BaseModel


class TgBillsBaseModel(BaseModel):
    payment_date: Optional[datetime] = None
    created_by: int
    s3_url: str
    plain_text: str
    file_name: str
    status: TgBillStatus
    payment_amount: Optional[float] = None
    counterparty_account_number: Optional[str] = None
    payment_purpose: Optional[str] = None
    counterparty_bank_bic: Optional[str] = None
    counterparty_name: Optional[str] = None
    corr_account: Optional[str] = None
    tochka_bank_account_id: Optional[int] = None


class TgBillsCreateModel(TgBillsBaseModel):
    pass


class TgBillsUpdateModel(TgBillsBaseModel):
    payment_date: Optional[datetime] = None
    created_by: Optional[int] = None
    s3_url: Optional[str] = None
    plain_text: Optional[str] = None
    file_name: Optional[str] = None
    status: Optional[TgBillStatus] = None
    payment_amount: Optional[float] = None
    counterparty_account_number: Optional[str] = None
    payment_purpose: Optional[str] = None
    counterparty_bank_bic: Optional[str] = None
    counterparty_name: Optional[str] = None
    corr_account: Optional[str] = None
    tochka_bank_account_id: Optional[int] = None


class TgBillsInDBBaseModel(TgBillsBaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None

    class Config:
        orm_mode = True


class TgBillsModel(TgBillsInDBBaseModel):
    pass


class TgBillsExtendedModel(TgBillsModel):
    accountId: Optional[str] = None
    request_id: Optional[str] = None
