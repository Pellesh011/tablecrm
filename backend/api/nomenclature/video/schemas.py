from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, validator


class NomenclatureVideoCreate(BaseModel):
    url: str
    description: Optional[str] = None
    tags: Optional[List[str]] = []

    @validator("url")
    def validate_url(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("URL видео не может быть пустым")
        if not v.lower().startswith(("http://", "https://")):
            raise ValueError("URL должен начинаться с http:// или https://")
        return v

    @validator("tags", pre=True, always=True)
    def normalize_tags(cls, v):
        if v is None:
            return []
        return [t.strip() for t in v if t and t.strip()]


class NomenclatureVideoPatch(BaseModel):
    url: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None

    @validator("url", pre=True, always=True)
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if not v:
            raise ValueError("URL видео не может быть пустым")
        if not v.lower().startswith(("http://", "https://")):
            raise ValueError("URL должен начинаться с http:// или https://")
        return v

    @validator("tags", pre=True, always=True)
    def normalize_tags(cls, v):
        if v is None:
            return v
        return [t.strip() for t in v if t and t.strip()]


class NomenclatureVideoRead(BaseModel):
    id: int
    nomenclature_id: int
    url: str
    description: Optional[str] = None
    tags: Optional[List[str]] = []
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class NomenclatureVideoList(BaseModel):
    result: List[NomenclatureVideoRead]
    count: int
