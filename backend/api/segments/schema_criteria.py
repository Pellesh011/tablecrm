from typing import List, Optional

from api.segments.schema_base import DateRange, Range
from pydantic import BaseModel, validator


class PurchaseCriteria(BaseModel):
    total_amount: Optional[Range]
    count: Optional[Range]
    last_purchase_days_ago: Optional[Range]
    amount_per_check: Optional[Range]
    date_range: Optional[DateRange]
    categories: Optional[List[str]]
    nomenclatures: Optional[List[str]]
    is_fully_paid: Optional[bool]
    count_of_goods: Optional[Range]

    @validator("categories", "nomenclatures", each_item=True)
    def validate_category_item(cls, v):
        if len(v) < 3:
            raise ValueError("Элемент списка должен быть не короче 3 символов")
        return v


class LoyalityCriteria(BaseModel):
    balance: Optional[Range]
    expires_in_days: Optional[Range]


class PickerCourierSchema(BaseModel):
    assigned: Optional[bool]
    start: Optional[DateRange]
    finish: Optional[DateRange]
    photos_not_added_minutes: Optional[int]


class RecipientInfoSchema(BaseModel):
    name: Optional[str]
    surname: Optional[str]
    phone: Optional[str]


class DeliverySchema(BaseModel):
    address: Optional[str]
    delivery_date: Optional[DateRange]
    note: Optional[str]
    recipient: Optional[RecipientInfoSchema]


class OrderCriteriaSchema(BaseModel):
    order_status: Optional[str]
    updated_at: Optional[DateRange]
    created_at: Optional[DateRange]


class NomenclatureCriteriaSchema(BaseModel):
    tags: Optional[List[str]]
    name: Optional[str]
    categories: Optional[List[str]]
    global_categories: Optional[List[str]]
    has_photo: Optional[bool]


class SegmentCriteria(BaseModel):
    purchases: Optional[PurchaseCriteria]
    loyality: Optional[LoyalityCriteria]
    tags: Optional[List[str]]
    docs_sales_tags: Optional[List[str]]
    delivery_required: Optional[bool]
    created_at: Optional[DateRange]
    picker: Optional[PickerCourierSchema]
    courier: Optional[PickerCourierSchema]
    delivery_info: Optional[DeliverySchema]
    nomenclatures: Optional[NomenclatureCriteriaSchema]
    orders: Optional[OrderCriteriaSchema]
