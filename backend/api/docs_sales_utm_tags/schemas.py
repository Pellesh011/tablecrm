from typing import List, Optional

from pydantic import BaseModel


class CreateUTMTag(BaseModel):
    utm_source: Optional[str]
    utm_medium: Optional[str]
    utm_campaign: Optional[str]
    utm_term: Optional[List[str]]
    utm_content: Optional[str]
    utm_name: Optional[str]
    utm_phone: Optional[str]
    utm_email: Optional[str]
    utm_leadid: Optional[str]
    utm_yclientid: Optional[str]
    utm_gaclientid: Optional[str]

    class Config:
        orm_mode = True


class UtmTag(CreateUTMTag):
    id: int
    docs_sales_id: int
