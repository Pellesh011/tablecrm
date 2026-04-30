from datetime import datetime
from typing import Optional

from database.db import TgBillApproveStatus
from pydantic import BaseModel


class TgBillApproversBaseModel(BaseModel):
    approver_id: int
    bill_id: int
    status: TgBillApproveStatus


class TgBillApproversCreateModel(TgBillApproversBaseModel):
    pass


class TgBillApproversUpdateModel(TgBillApproversBaseModel):
    approver_id: Optional[int] = None
    bill_id: Optional[int] = None
    status: Optional[TgBillApproveStatus] = None


class TgBillApproversInDBBaseModel(TgBillApproversBaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None

    class Config:
        orm_mode = True


class TgBillApproversModel(TgBillApproversInDBBaseModel):
    pass


class TgBillApproversExtendedModel(TgBillApproversModel):
    username: str
