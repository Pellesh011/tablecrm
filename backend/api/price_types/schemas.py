import re
from typing import List, Optional

from pydantic import BaseModel, validator


class PriceTypeCreate(BaseModel):
    name: str
    tags: Optional[List[str]] = []

    class Config:
        orm_mode = True

    @validator("tags")
    def validate_tags(cls, tag_list):
        if tag_list is None:
            return []
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


class PriceTypeEdit(PriceTypeCreate):
    name: Optional[str]
    tags: Optional[List[str]] = None


class PriceType(PriceTypeCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class PriceTypeList(BaseModel):
    __root__: Optional[List[PriceType]]

    class Config:
        orm_mode = True


class PriceTypeListGet(BaseModel):
    result: Optional[List[PriceType]]
    count: int
