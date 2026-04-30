from datetime import datetime
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, root_validator, validator


class UpdateSettings(BaseModel):
    interval_minutes: int

    @validator("interval_minutes")
    def validate_interval_minutes(cls, v):
        if v < 5:
            raise ValueError("Интервал не может быть менее 5 минут")
        return v


class SegmentBaseCreate(BaseModel):
    name: str
    description: Optional[str]
    type_of_update: Literal["cron", "request"]
    update_settings: Optional[UpdateSettings]

    is_archived: bool

    @root_validator(pre=True)
    def check_update_settings(cls, values):
        values = cls._remove_empty_fields(values)
        update_type = values.get("type_of_update")
        settings = values.get("update_settings")

        if update_type == "cron" and settings is None:
            raise ValueError(
                "Поле 'update_settings' обязательно при type_of_update='cron'"
            )
        return values

    @classmethod
    def _remove_empty_fields(cls, data):
        """Чистим переданные пустые поля  {}, []"""
        if isinstance(data, dict):
            cleaned = {}
            for k, v in data.items():
                v_cleaned = cls._remove_empty_fields(v)
                # Удаляем только по-настоящему пустые: None, {}, []
                if v_cleaned is not None and v_cleaned != {} and v_cleaned != []:
                    cleaned[k] = v_cleaned
            return cleaned

        elif isinstance(data, list):
            cleaned_list = [cls._remove_empty_fields(item) for item in data]
            # Удаляем только полностью пустые элементы
            return [
                item
                for item in cleaned_list
                if item != {} and item != [] and item is not None
            ]

        # Базовые значения сохраняем всегда: 0, False, "", и т.д.
        return data


class Range(BaseModel):
    gte: Optional[float]
    lte: Optional[float]
    eq: Optional[float]
    is_none: Optional[bool]


class DateRange(BaseModel):
    gte: Optional[str]
    lte: Optional[str]
    gte_seconds_ago: Optional[int]
    lte_seconds_ago: Optional[int]
    is_none: Optional[bool]

    @validator("gte", "lte")
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d").date()
            return v
        except ValueError:
            raise ValueError("Дата должна быть в формате YYYY-MM-DD")


class SegmentType(Enum):
    market = 1
    standard = 2
