import re
from typing import List, Optional

from pydantic import BaseModel, Field, validator


class CashboxUpdate(BaseModel):
    tags: Optional[List[str]]
    status: Optional[bool]

    timezone: Optional[str]
    payment_past_edit_days: Optional[int] = Field(default=None, ge=0)

    @validator("payment_past_edit_days")
    def validate_payment_past_edit_days(cls, days):
        if isinstance(days, int):
            if days < 0:
                return None
        return days

    @validator("tags")
    def validate_tags(cls, tag_list):
        if tag_list is None:
            return tag_list
        if len(tag_list) > 10:
            raise ValueError("Максимум 10 тегов")

        if len(set(tag_list)) < len(tag_list):
            raise ValueError("Теги не должны повторяться")

        pattern = re.compile(r"^[a-zA-Zа-яА-Я0-9_-]{2,20}$")
        for tag in tag_list:
            if not pattern.match(tag):
                raise ValueError(
                    f"Тег '{tag}' содержит недопустимые символы или некорректную длину (2–20 символов)"
                )

        return tag_list
