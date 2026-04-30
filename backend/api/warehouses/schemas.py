from typing import List, Optional

from pydantic import BaseModel


class WarehouseCreate(BaseModel):
    name: str
    type: Optional[str]
    description: Optional[str]
    address: Optional[str]
    phone: Optional[str]
    parent: Optional[int]
    is_public: Optional[bool]
    status: bool = True

    class Config:
        orm_mode = True


class WarehouseCreateMass(BaseModel):
    __root__: List[WarehouseCreate]

    class Config:
        orm_mode = True


class WarehouseEdit(WarehouseCreate):
    name: Optional[str]
    status: Optional[bool]


class Warehouse(WarehouseCreate):
    id: int
    updated_at: int
    created_at: int
    longitude: Optional[float]
    latitude: Optional[float]
    qr_hash: Optional[str] = None
    qr_url: Optional[str] = None

    class Config:
        orm_mode = True


class WarehouseList(BaseModel):
    __root__: Optional[List[Warehouse]]

    class Config:
        orm_mode = True


class WarehouseListGet(BaseModel):
    result: Optional[List[Warehouse]]
    count: int
