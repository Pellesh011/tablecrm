from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel


class OauthBaseModel(BaseModel):
    id: int = None
    cashbox_id: Optional[int] = None
    access_token: str = None
    is_deleted: bool = False
    warehouse_id: Optional[int] = None


class OauthModel(OauthBaseModel):
    created_at: datetime
    updated_at: datetime


class OauthUpdateModel(OauthBaseModel):
    cashbox_id: Optional[int] = None
    access_token: Optional[str] = None
    warehouse_id: Optional[int] = None
    is_deleted: bool = None


class OauthWarehouseModel(BaseModel):
    warehouse_name: str = None
    warehouse_description: str = None
    last_update: datetime = None
    warehouse_id: int = None
    status: bool = None


class SettingsFiscalization(BaseModel):
    provider: Literal[
        "avanpost",
        "fns",
        "a_qsi",
        "atol",
        "business_ru",
        "digital_kassa",
        "evotor",
        "first_ofd",
        "kit_invest",
        "komtet",
        "life_pay",
        "mertrade",
        "modul_kassa",
        "rocket",
        "shtrih_m",
    ]
    enabled: bool


class OauthSettings(BaseModel):
    account_id: str
    test: bool
    fiscalization: Optional[SettingsFiscalization] = None
    fiscalization_enabled: Optional[bool] = None
    payment_methods: List[str] = None
    status: Literal["enabled", "disabled"]
