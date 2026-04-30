from typing import Optional

from pydantic import BaseModel


class SellerUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class SellerResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    photo: Optional[str] = None
