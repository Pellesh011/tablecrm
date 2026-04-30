from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class BlogFolderCreate(BaseModel):
    name: str
    slug: str
    parent_id: Optional[int] = None
    settings: Optional[Dict[str, Any]] = None


class BlogFolderUpdate(BaseModel):
    name: Optional[str] = None
    slug: Optional[str] = None
    parent_id: Optional[int] = None
    settings: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class BlogSiteUpdate(BaseModel):
    name: Optional[str] = None
    is_active: Optional[bool] = None


class BlogFolderOut(BaseModel):
    id: int
    cashbox_id: int
    name: str
    slug: str
    parent_id: Optional[int]
    path: str
    settings: Optional[Dict[str, Any]]
    is_active: bool


class BlogSiteCreate(BaseModel):
    name: str


class BlogSiteOut(BaseModel):
    id: int
    cashbox_id: int
    name: str
    token: str
    is_active: bool


class BlogSiteFoldersSet(BaseModel):
    folder_ids: List[int] = Field(default_factory=list)


class BlogPostCreate(BaseModel):
    folder_id: int
    title: str
    slug: str
    content: str
    excerpt: Optional[str] = None
    status: Optional[str] = "DRAFT"
    published_at: Optional[datetime] = None
    meta: Optional[Dict[str, Any]] = None
    tag_slugs: Optional[List[str]] = None


class BlogPostUpdate(BaseModel):
    folder_id: Optional[int] = None
    title: Optional[str] = None
    slug: Optional[str] = None
    excerpt: Optional[str] = None
    content: Optional[str] = None
    status: Optional[str] = None
    published_at: Optional[datetime] = None
    meta: Optional[Dict[str, Any]] = None
    tag_slugs: Optional[List[str]] = None
    is_deleted: Optional[bool] = None


class BlogPostOut(BaseModel):
    id: int
    cashbox_id: int
    folder_id: int
    title: str
    slug: str
    excerpt: Optional[str]
    content: str
    status: str
    published_at: Optional[datetime]
    meta: Optional[Dict[str, Any]]
    is_deleted: bool
    tags: List[str] = Field(default_factory=list)
    pictures: List[str] = Field(default_factory=list)
