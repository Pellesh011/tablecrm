from typing import Any, Dict, Optional

from pydantic import BaseModel


class AvitoWebhookValue(BaseModel):
    """Значение payload.value из webhook Авито."""

    id: Optional[str] = None
    chat_id: Optional[str] = None
    user_id: Optional[int] = None
    author_id: Optional[int] = None
    created: Optional[int] = None
    published_at: Optional[str] = None
    type: Optional[str] = None
    chat_type: Optional[str] = None
    item_id: Optional[int] = None
    content: Optional[Dict[str, Any]] = None
    read: Optional[int] = None


class AvitoWebhookPayload(BaseModel):
    """Webhook payload"""

    type: str
    value: AvitoWebhookValue


class AvitoWebhook(BaseModel):
    """Complete webhook from Avito"""

    id: str
    version: str
    timestamp: int
    payload: AvitoWebhookPayload
