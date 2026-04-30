"""Схемы для CommerceML подключений"""

from typing import Optional

from pydantic import BaseModel, Field


class CommerceMLConnectionCreate(BaseModel):
    name: str = Field(..., description="Название подключения")
    url: Optional[str] = Field("", description="URL внешней системы (опционально)")
    username: Optional[str] = Field(
        None, description="Логин для Basic Auth (если не указан — генерируется)"
    )
    password: Optional[str] = Field(
        None, description="Пароль для Basic Auth (если не указан — генерируется)"
    )
    active: Optional[bool] = None
    import_products: Optional[bool] = None
    export_products: Optional[bool] = None
    import_orders: Optional[bool] = None
    export_orders: Optional[bool] = None


class CommerceMLConnectionUpdate(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    active: Optional[bool] = None
    is_active: Optional[bool] = None  # алиас для active (фронт может слать любой)
    import_products: Optional[bool] = None
    export_products: Optional[bool] = None
    import_orders: Optional[bool] = None
    export_orders: Optional[bool] = None


class CommerceMLResetPassword(BaseModel):
    """Тело запроса сброса пароля. Если password не указан — генерируется случайный."""

    password: Optional[str] = None


class CommerceMLConnection(BaseModel):
    id: int
    cashbox_id: int
    name: str
    url: str
    username: str
    password: str
    active: bool
    is_active: bool = False  # дублирует active для фронта (модалка «Активно»)
    import_products: bool
    export_products: bool
    import_orders: bool
    export_orders: bool
    products_loaded_count: int = 0
    orders_exported_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        orm_mode = True
