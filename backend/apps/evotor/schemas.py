from typing import List, Optional

from pydantic import BaseModel, Field


class EvotorInstallEvent(BaseModel):
    id: Optional[str] = None
    timestamp: Optional[int] = None
    version: Optional[float] = None
    type: Optional[str] = None
    data: Optional[dict] = None


class EvotorHeader(BaseModel):
    user_agent: str = None
    authorization: str = None


class EvotorUserToken(BaseModel):
    userId: str
    evotor_token: str = Field(alias="token")


class EvotorNomenclature(BaseModel):
    uuid: str
    code: str
    barCodes: List[str]
    alcoCodes: List[str]
    name: str
    price: float
    quantity: int
    costPrice: float
    measureName: str
    tax: str
    allowToSell: bool
    description: str
    articleNumber: str
    parentUuid: str
    group: bool
    type: str
    alcoholByVolume: float
    alcoholProductKindCode: int
    tareVolume: float


class ListEvotorNomenclature(BaseModel):
    __root__: List[EvotorNomenclature]
