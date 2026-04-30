# api/segments/schemas.py

from datetime import datetime
from typing import List, Optional, Union

from api.segments.schema_actions import AutoReplyConfig, SegmentActions
from api.segments.schema_base import SegmentBaseCreate
from api.segments.schema_criteria import SegmentCriteria
from api.segments.segment_result import SegmentContragentData
from api.tags import schemas as tags_schemas
from pydantic import BaseModel

SegmentData = Union[SegmentContragentData,]


class Segment(BaseModel):
    id: int
    name: str
    description: Optional[str]
    tags: Optional[List[tags_schemas.Tag]]
    criteria: Optional[dict] = None
    actions: Optional[dict] = None
    auto_reply: Optional[dict] = None
    updated_at: Optional[datetime] = None
    type_of_update: str
    update_settings: Optional[dict]
    status: str
    is_archived: bool
    selection_field: Optional[str] = None
    type: Optional[int] = None


class SegmentCreate(SegmentBaseCreate):
    criteria: Optional[SegmentCriteria] = None
    tags_ids: Optional[List[int]] = None
    actions: Optional[SegmentActions] = None
    auto_reply: Optional[AutoReplyConfig] = None
    type: Optional[int] = 2


class SegmentWithContragents(Segment):
    contragents_count: Optional[int] = 0
    added_contragents_count: Optional[int] = 0
    deleted_contragents_count: Optional[int] = 0
    entered_contragents_count: Optional[int] = 0
    exited_contragents_count: Optional[int] = 0
