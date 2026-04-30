from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class ShiftStatus(str, Enum):
    on_shift = "on_shift"
    off_shift = "off_shift"
    on_break = "on_break"


class StartShiftRequest(BaseModel):
    """Запрос на начало смены"""

    pass  # Пока что дополнительных параметров не нужно


class EndShiftRequest(BaseModel):
    """Запрос на завершение смены"""

    pass  # Пока что дополнительных параметров не нужно


class CreateBreakRequest(BaseModel):
    """Запрос на создание перерыва"""

    duration_minutes: int

    class Config:
        schema_extra = {"example": {"duration_minutes": 30}}


class ShiftResponse(BaseModel):
    """Ответ с информацией о смене"""

    id: int
    user_id: int
    cashbox_id: int
    shift_start: datetime
    shift_end: Optional[datetime] = None
    status: ShiftStatus
    break_start: Optional[datetime] = None
    break_duration: Optional[int] = None
    created_at: datetime
    updated_at: datetime


class ShiftStatusResponse(BaseModel):
    """Ответ с текущим статусом смены"""

    is_on_shift: bool
    status: ShiftStatus
    current_shift: Optional[ShiftResponse] = None
    message: str


class ShiftData(BaseModel):
    """Схема отражающая модель в БД"""

    user_id: int
    cashbox_id: int
    shift_start: datetime
    shift_end: Optional[datetime] = None
    status: ShiftStatus
    break_start: Optional[datetime] = None
    break_duration: Optional[int] = None
    created_at: datetime
    updated_at: datetime


class ShiftEvent(BaseModel):
    first_name: str
    last_name: Optional[str]
    username: Optional[str]
    photo: str
    relation_id: Optional[int] = None
    phone_number: Optional[str]

    shift_status: ShiftStatus
    event_start: datetime

    class Config:
        orm_mode = True


class ShiftEventsList(BaseModel):
    result: List[ShiftEvent]
    total_count: int


class ShiftEventCreate(BaseModel):
    relation_id: int
    cashbox_id: int
    shift_status: str
    event_start: datetime
    event_end: Optional[datetime] = None
