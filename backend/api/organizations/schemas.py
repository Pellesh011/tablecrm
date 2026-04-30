from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class OrgType(str, Enum):
    ooo = "ООО"
    ip = "ИП"
    self_employed = "Самозанятый"


class TaxType(str, Enum):
    osno = "ОСНО"
    usn = "УСН"
    ausn = "АУСН"
    ecxn = "ЕСХН"
    patent = "Патент"


class OrganizationCreate(BaseModel):
    type: str
    short_name: str
    full_name: Optional[str]
    work_name: Optional[str]
    prefix: Optional[str]
    inn: Optional[int]
    kpp: Optional[int]
    okved: Optional[int]
    okved2: Optional[int]
    okpo: Optional[int]
    ogrn: Optional[int]
    org_type: Optional[OrgType]
    tax_type: Optional[TaxType]
    tax_percent: Optional[float]
    registration_date: Optional[int]

    class Config:
        orm_mode = True


class OrganizationEdit(OrganizationCreate):
    type: Optional[str]
    short_name: Optional[str]


class Organization(OrganizationCreate):
    id: int
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class OrganizationList(BaseModel):
    __root__: Optional[List[Organization]]

    class Config:
        orm_mode = True


class OrganizationListGet(BaseModel):
    result: Optional[List[Organization]]
    count: int
