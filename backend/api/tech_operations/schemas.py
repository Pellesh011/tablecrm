from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class TechOpStatus(str, Enum):
    active = "active"
    reversed = "reversed"
    canceled = "canceled"
    deleted = "deleted"


class TechOperationComponentCreate(BaseModel):
    name: str = Field(..., min_length=1)
    quantity: float
    nomeclature_id: int
    gross_weight: Optional[float] = None
    net_weight: Optional[float] = None


class TechOperationComponent(TechOperationComponentCreate):
    id: UUID
    operation_id: UUID

    class Config:
        orm_mode = True


class TechOperationCreate(BaseModel):
    """Ручное создание тех операции."""

    tech_card_id: UUID
    output_quantity: float = Field(..., gt=0)
    from_warehouse_id: int
    to_warehouse_id: int
    nomenclature_id: int
    component_quantities: List[TechOperationComponentCreate] = Field(
        default_factory=list
    )
    payment_ids: Optional[List[UUID]] = None
    docs_sales_id: Optional[int] = None


class TechOperation(BaseModel):
    id: UUID
    tech_card_id: UUID
    output_quantity: float
    from_warehouse_id: int
    to_warehouse_id: int
    nomenclature_id: Optional[int] = None
    user_id: int
    cashbox_id: Optional[int] = None
    status: TechOpStatus
    production_doc_id: Optional[int] = None
    consumption_doc_id: Optional[int] = None
    sale_write_off_doc_id: Optional[int] = None
    docs_sales_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    production_order_id: Optional[UUID] = None
    consumption_order_id: Optional[UUID] = None

    component_quantities: List[TechOperationComponent] = Field(default_factory=list)
    payment_ids: List[UUID] = Field(default_factory=list)

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TechOperationReverseResponse(BaseModel):
    """Ответ на распроведение тех операции."""

    success: bool
    operation_id: UUID
    reversed_consumption_doc_id: Optional[int]
    reversed_production_doc_id: Optional[int]
    reversed_sale_write_off_doc_id: Optional[int]
    message: str
