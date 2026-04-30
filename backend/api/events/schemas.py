import datetime
from typing import List, Optional, Union

from pydantic import BaseModel


class Event(BaseModel):
    id: int
    type: Optional[str]
    name: Optional[str]
    method: Optional[str]
    url: Optional[str]
    payload: Optional[Union[dict, List]]
    cashbox_id: Optional[int]
    user_id: Optional[int]
    token: Optional[str]
    ip: Optional[str]
    request_time: Optional[float]
    promoimage: Optional[str]
    promodata: Optional[dict]

    created_at: datetime.datetime
    updated_at: Union[datetime.datetime, None]


class GetEvents(BaseModel):
    result: Optional[List[Event]]
    count: int
