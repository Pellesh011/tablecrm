from datetime import datetime

from database.enums import TriggerTime, TriggerType
from pydantic import BaseModel


class Filtersamobot(BaseModel):
    name: str = None


class AmoBotView(BaseModel):
    id: int
    name: str


class ViewTrigger(BaseModel):
    id: int
    name: str
    amo_bot: AmoBotView
    type: TriggerType
    time_variant: TriggerTime
    time: int
    active: bool = False
    created_at: datetime
    updated_at: datetime


class CreateTrigger(BaseModel):
    name: str
    amo_bots_id: int
    type: TriggerType
    time_variant: TriggerTime
    time: int
    active: bool = False


class PatchTrigger(BaseModel):
    name: str = None
    amo_bots_id: int = None
    type: TriggerType = None
    time_variant: TriggerTime = None
    time: int = None
    active: bool = None
