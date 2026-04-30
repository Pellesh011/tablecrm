from datetime import datetime
from typing import List, Optional

from database import db
from pydantic import BaseModel, validator


class FolderEntity(BaseModel):
    entity_id: int
    entity_type: str

    @validator("entity_type")
    def validate_entity_type(cls, v):
        if v not in dir(db):
            raise ValueError("Такой сущности не существует!")

        return v


class FolderEntityView(FolderEntity):
    entity_data: dict


class FolderEntityList(BaseModel):
    entity_type: str
    entities_data: List[dict]
    count: int


class FolderCreate(BaseModel):
    name: str
    slug: str
    previous_id: Optional[int]
    is_active: bool


class Folder(BaseModel):
    id: int
    name: str
    slug: str
    previous_id: Optional[int]
    is_deleted: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class FolderList(BaseModel):
    items: Optional[List[Folder]]
    count: int
