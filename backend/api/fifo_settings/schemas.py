from typing import Optional

from pydantic import BaseModel


class Edit(BaseModel):
    fully_closed_date: Optional[int]
    temporary_closed_date: Optional[int]
    blocked_date: Optional[int]
    month_closing_delay_days: Optional[int]
    preview_close_period_seconds: Optional[int]


class Create(Edit):
    fully_closed_date: int = 0
    preview_close_period_seconds: int = 86400
    organization_id: int

    class Config:
        orm_mode = True


class Edit(BaseModel):
    fully_closed_date: Optional[int]
    temporary_closed_date: Optional[int]
    blocked_date: Optional[int]
    month_closing_delay_days: Optional[int]
    preview_close_period_seconds: Optional[int]


class View(Create):
    id: int
    updated_at: int
    created_at: int
