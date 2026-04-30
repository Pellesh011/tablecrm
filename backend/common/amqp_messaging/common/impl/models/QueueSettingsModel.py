from typing import Optional

from pydantic import BaseModel


class QueueSettingsModel(BaseModel):
    queue_name: str
    prefetch_count: Optional[int] = 0
