"""Pydantic схемы для категорий (public_categories service)"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class GlobalCategoryBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    name: str
    description: Optional[str] = None
    code: Optional[int] = None
    parent_id: Optional[int] = None
    external_id: Optional[str] = None
    image_url: Optional[str] = None
    is_active: bool = True


class GlobalCategoryCreate(GlobalCategoryBase):
    pass


class GlobalCategoryUpdate(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    name: Optional[str] = None
    description: Optional[str] = None
    code: Optional[int] = None
    parent_id: Optional[int] = None
    external_id: Optional[str] = None
    image_url: Optional[str] = None
    is_active: Optional[bool] = None


class GlobalCategory(GlobalCategoryBase):
    id: int
    created_at: str
    updated_at: str
    has_products: Optional[bool] = (
        None  # Показывает, есть ли актуальные товары в категории
    )


class GlobalCategoryTree(GlobalCategory):
    children: Optional[List["GlobalCategoryTree"]] = []  # noqa: E501
    processing_time_ms: Optional[int] = None


class GlobalCategoryList(BaseModel):
    result: List[GlobalCategory]
    count: int
    processing_time_ms: Optional[int] = None


class GlobalCategoryTreeList(BaseModel):
    result: List[GlobalCategoryTree]
    count: int
    processing_time_ms: Optional[int] = None


class TreeSelectNode(BaseModel):
    """Узел для Ant Design TreeSelect"""

    title: str
    value: int
    children: Optional[List["TreeSelectNode"]] = []


class TreeSelectNodeList(BaseModel):
    result: List[TreeSelectNode]
    count: int
    processing_time_ms: Optional[int] = None
