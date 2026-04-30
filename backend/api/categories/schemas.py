from typing import List, Optional

from pydantic import BaseModel


class CategoryCreate(BaseModel):
    name: str
    description: Optional[str]
    code: Optional[int]
    parent: Optional[int]
    status: bool = True
    photo_id: Optional[int]
    external_id: Optional[str]

    class Config:
        orm_mode = True


class CategoryCreateMass(BaseModel):
    __root__: List[CategoryCreate]

    class Config:
        orm_mode = True


class CategoryEdit(CategoryCreate):
    name: Optional[str]
    status: Optional[bool]


class Category(CategoryCreate):
    id: int
    updated_at: int
    created_at: int
    picture: Optional[str]

    class Config:
        orm_mode = True


class CategoryList(BaseModel):
    __root__: Optional[List[Category]]

    class Config:
        orm_mode = True


class CategoryListGet(BaseModel):
    result: Optional[List[Category]]
    count: int


class CategoryTree(BaseModel):
    key: int
    name: str
    nom_count: int
    description: Optional[str]
    code: Optional[int]
    status: bool = True
    parent: Optional[int]
    children: Optional[List["CategoryTree"]]
    expanded_flag: bool
    picture: Optional[str]
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class CategoryTreeGet(BaseModel):
    result: Optional[List[CategoryTree]]
    count: int
