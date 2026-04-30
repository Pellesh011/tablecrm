from typing import Dict, List, Optional

from pydantic import BaseModel


class PictureEdit(BaseModel):
    is_main: bool


class Picture(BaseModel):
    id: int
    entity: str
    entity_id: int
    is_main: Optional[bool]
    url: str
    public_url: str
    size: Optional[int]
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class PictureList(BaseModel):
    __root__: Optional[List[Picture]]

    class Config:
        orm_mode = True


class PictureListGet(BaseModel):
    result: Optional[List[Picture]]
    count: int


class PictureBatchRequest(BaseModel):
    entity: str
    entity_ids: List[int]


class PictureBatchResponse(BaseModel):
    result: Dict[int, List[Picture]]
    count: int
    processing_time_ms: int
