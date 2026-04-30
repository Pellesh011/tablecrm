from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CashboxSettingsView(BaseModel):
    cashbox_id: int
    require_photo_for_writeoff: bool
    check_nomenclature_duplicates_by_name: bool
    check_nomenclature_duplicates_by_code: bool
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    class Config:
        orm_mode = True


class CreateCashboxSettings(BaseModel):
    require_photo_for_writeoff: bool = False
    check_nomenclature_duplicates_by_name: bool = False
    check_nomenclature_duplicates_by_code: bool = False


class PatchCashboxSettings(BaseModel):
    require_photo_for_writeoff: Optional[bool] = None
    check_nomenclature_duplicates_by_name: Optional[bool] = None
    check_nomenclature_duplicates_by_code: Optional[bool] = None
    is_deleted: Optional[bool] = None
