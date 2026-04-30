from uuid import UUID

from pydantic import BaseModel


class BaseModelMessage(BaseModel):
    message_id: UUID
