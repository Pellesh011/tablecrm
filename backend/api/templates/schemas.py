from typing import List, Optional

from pydantic import BaseModel


class Tag(BaseModel):
    id: int
    name: str

    class Config:
        orm_mode = True


class TypeTemplate(BaseModel):
    id: int
    name: str

    class Config:
        orm_mode = True


class DocTemplateCreate(BaseModel):
    name: str
    description: Optional[str] = None
    tags: Optional[str] = None
    user_id: int
    is_deleted: bool = False
    type: int = None

    class Config:
        orm_mode = True


class DocTemplateUpdate(BaseModel):
    id: Optional[int]
    name: Optional[str]
    description: Optional[str] = None
    template_data: Optional[str] = None
    tags: Optional[str] = None
    user_id: Optional[int]
    is_deleted: Optional[bool] = False
    type: Optional[int] = None

    class Config:
        orm_mode = True


class DocTemplate(BaseModel):
    id: int = None
    name: str = None
    description: Optional[str] = None
    tags: Optional[str] = None
    user_id: str = None
    created_at: int = None
    updated_at: int = None
    is_deleted: Optional[bool]
    type: int = None

    class Config:
        orm_mode = True


class Page(BaseModel):
    name: str
    id: int

    class Config:
        orm_mode = True


class Area(Page):
    pass


class DocTemplateFull(DocTemplate):
    template_data: str = None
    pages: Optional[List[Page]] = None
    areas: Optional[List[Area]] = None

    class Config:
        orm_mode = True


class TemplateList(BaseModel):
    result: Optional[List[DocTemplate]]
    tags: Optional[str] = None


class TemplateCreate(BaseModel):
    areas: Optional[List[int]] = None
    pages: Optional[List[int]] = None


class TemptalePatchBody(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[str] = None
    user_id: Optional[int] = None
    is_deleted: Optional[bool] = None
    type: Optional[int] = None
