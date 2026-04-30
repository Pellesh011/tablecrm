from typing import Optional

from pydantic import BaseModel


class Account(BaseModel):
    customerCode: Optional[str] = None
    accountId: Optional[str] = None
    transitAccount: Optional[str] = None
    status: Optional[str] = None
    currency: Optional[str] = None
    is_deleted: Optional[bool] = None
    is_active: Optional[bool] = None

    class Config:
        orm_mode = True


class AccountUpdate(BaseModel):
    is_deleted: Optional[bool] = None
    is_active: Optional[bool] = None

    class Config:
        orm_mode = True


class StatementData(BaseModel):
    accountId: str
    startDateTime: str
    endDateTime: str

    class Config:
        orm_mode = True
