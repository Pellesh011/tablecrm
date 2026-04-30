from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator


class TechCardType(str, Enum):
    reference = "reference"
    automatic = "automatic"


class TechCardMode(str, Enum):
    """Режим работы тех карты."""

    reference = "reference"
    semi_auto = "semi_auto"
    auto = "auto"


class TechCardStatus(str, Enum):
    active = "active"
    canceled = "canceled"
    deleted = "deleted"


class TechCardItemCreate(BaseModel):
    """Компонент (сырьё) тех карты."""

    nomenclature_id: int
    type_of_processing: Optional[str] = None
    net_weight: Optional[float] = None
    waste_from_cold_processing: Optional[float] = None
    waste_from_heat_processing: Optional[float] = None
    quantity: float = Field(..., gt=0)
    gross_weight: Optional[float] = None
    output: Optional[float] = None


class TechCardItem(TechCardItemCreate):
    id: UUID
    tech_card_id: UUID

    class Config:
        orm_mode = True


class TechCardOutputItemCreate(BaseModel):
    """Выходное изделие тех карты (то, что производится)."""

    nomenclature_id: int
    quantity: float = Field(..., gt=0)
    unit_id: Optional[int] = None


class TechCardOutputItem(TechCardOutputItemCreate):
    id: UUID
    tech_card_id: UUID

    class Config:
        orm_mode = True


class TechCardBase(BaseModel):
    name: str = Field(..., min_length=1)
    description: Optional[str] = None
    card_type: TechCardType
    card_mode: TechCardMode = TechCardMode.reference
    auto_produce: bool = False
    parent_nomenclature_id: Optional[int] = None

    # Склады (обязательны для semi_auto и auto)
    warehouse_from_id: Optional[int] = None  # склад сырья
    warehouse_to_id: Optional[int] = None  # склад готовой продукции

    @validator("warehouse_from_id", always=True)
    def validate_warehouse_from(cls, v, values):
        mode = values.get("card_mode")
        if mode in (TechCardMode.semi_auto, TechCardMode.auto) and not v:
            raise ValueError(f"warehouse_from_id обязателен для режима '{mode}'")
        return v

    @validator("warehouse_to_id", always=True)
    def validate_warehouse_to(cls, v, values):
        mode = values.get("card_mode")
        if mode == TechCardMode.auto and not v:
            raise ValueError("warehouse_to_id обязателен для режима 'auto'")
        return v


class TechCardCreate(TechCardBase):
    """Создание тех карты."""

    items: List[TechCardItemCreate] = Field(default_factory=list)  # сырьё
    output_items: List[TechCardOutputItemCreate] = Field(default_factory=list)  # выход


class TechCardUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    card_mode: Optional[TechCardMode] = None
    auto_produce: Optional[bool] = None
    parent_nomenclature_id: Optional[int] = None
    warehouse_from_id: Optional[int] = None
    warehouse_to_id: Optional[int] = None
    items: Optional[List[TechCardItemCreate]] = None
    output_items: Optional[List[TechCardOutputItemCreate]] = None
    is_archived: Optional[bool] = None


class TechCard(TechCardBase):
    id: UUID
    created_at: datetime
    updated_at: datetime
    user_id: Optional[int] = None
    cashbox_id: Optional[int] = None
    status: TechCardStatus

    class Config:
        orm_mode = True


class TechCardResponse(TechCard):
    items: List[TechCardItem] = []
    output_items: List[TechCardOutputItem] = []
