from typing import List, Optional

from pydantic import BaseModel


class ManufacturerCreate(BaseModel):
    name: str
    photo_id: Optional[int]
    external_id: Optional[str]

    class Config:
        orm_mode = True


class ManufacturerCreateMass(BaseModel):
    __root__: List[ManufacturerCreate]

    class Config:
        orm_mode = True


class ManufacturerEdit(ManufacturerCreate):
    name: Optional[str]


class Manufacturer(ManufacturerCreate):
    id: int
    updated_at: int
    created_at: int
    picture: Optional[str]

    class Config:
        orm_mode = True


class ManufacturerList(BaseModel):
    __root__: Optional[List[Manufacturer]]

    class Config:
        orm_mode = True


class ManufacturerListGet(BaseModel):
    result: Optional[List[Manufacturer]]
    count: int
