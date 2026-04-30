from enum import Enum
from typing import Optional

from api.segments.schema_actions import SegmentActions
from api.segments.schema_criteria import SegmentCriteria
from pydantic import BaseModel


class WebhookEventType(str, Enum):
    SEGMENTS_START = "segment.start"


class WebhookPayload(BaseModel):
    event: WebhookEventType
    data: dict


class PayloadData(BaseModel):
    cashbox_id: int


class SegmentWebhookPayloadData(PayloadData):
    segment_id: int
    criteria: SegmentCriteria
    actions: Optional[SegmentActions]
