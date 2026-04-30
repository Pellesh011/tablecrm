from typing import List, Literal, Optional

from pydantic import BaseModel, Field, validator


class TagList(BaseModel):
    __root__: List[str]

    @validator("__root__", pre=True)
    def validate_tags(cls, v):
        if isinstance(v, str):
            v = [t.strip() for t in v.split(",") if t.strip()]
        if not isinstance(v, list):
            raise ValueError("Tags must be a list or comma-separated string")
        cleaned = []
        for tag in v:
            if not isinstance(tag, str):
                raise ValueError("Each tag must be a string")
            tag = tag.strip().lower()
            if (
                not tag
                or len(tag) > 50
                or not tag.replace("-", "").replace("_", "").isalnum()
            ):
                raise ValueError(f"Invalid tag: {tag}")
            cleaned.append(tag)
        return cleaned


class FileCreate(BaseModel):
    title: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    tags: Optional[TagList] = None


class FileUpdate(BaseModel):
    title: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    tags: Optional[TagList] = None


class FileFiltersQuery(BaseModel):
    search: Optional[str] = Field(None, max_length=100)
    tags: Optional[str] = Field(
        None, description="Comma-separated tags: invoice,paid,2025"
    )
    created_from: Optional[int] = Field(None, ge=0)
    created_to: Optional[int] = Field(None, ge=0)
    updated_from: Optional[int] = Field(None, ge=0)
    updated_to: Optional[int] = Field(None, ge=0)
    sort: Literal[
        "created_at_asc", "created_at_desc", "updated_at_asc", "updated_at_desc"
    ] = "created_at_desc"


class FileResponse(BaseModel):
    id: int
    title: Optional[str]
    description: Optional[str]
    tags: Optional[List[str]]
    url: str
    public_url: str
    size: Optional[int]
    mime_type: str
    extension: str
    created_at: int
    updated_at: int

    class Config:
        orm_mode = True


class FileListResponse(BaseModel):
    result: List[FileResponse]
    count: int
