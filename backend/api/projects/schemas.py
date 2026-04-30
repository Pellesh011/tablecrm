from typing import List, Optional

from pydantic import BaseModel


class Project(BaseModel):
    id: int
    external_id: Optional[str]
    name: str
    incoming: float
    outgoing: float
    profitability: float
    proj_sum: float
    icon_file: Optional[str]
    created_at: int
    updated_at: int

    class Config:
        orm_mode = True


class ProjectEdit(BaseModel):
    id: int
    name: Optional[str]
    external_id: Optional[str]
    proj_sum: Optional[float]

    class Config:
        orm_mode = True


class ProjectCreate(BaseModel):
    name: str
    external_id: Optional[str]
    proj_sum: float

    class Config:
        orm_mode = True


class GetProjects(BaseModel):
    result: Optional[List[Project]]
    count: int

    class Config:
        orm_mode = True
