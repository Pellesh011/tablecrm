from typing import List, Optional

from pydantic import BaseModel


class AvitoCredentialsCreate(BaseModel):
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    redirect_uri: Optional[str] = None
    channel_name: Optional[str] = None
    grant_type: Optional[str] = "client_credentials"

    class Config:
        json_schema_extra = {
            "example": {
                "api_key": "your_client_id",
                "api_secret": "your_client_secret",
                "redirect_uri": "https://<APP_URL>/api/v1/hook/chat/123456",
                "channel_name": "Avito Москва",
                "grant_type": "client_credentials",
            }
        }


class AvitoWebhookResponse(BaseModel):
    success: bool
    message: str
    chat_id: Optional[int] = None
    message_id: Optional[int] = None


class AvitoConnectResponse(BaseModel):
    success: bool
    message: str
    channel_id: int
    cashbox_id: int
    authorization_url: Optional[str] = None
    webhook_registered: Optional[bool] = None
    webhook_url: Optional[str] = None
    webhook_error: Optional[str] = None


class AvitoHistoryLoadResponse(BaseModel):
    success: bool
    channel_id: int
    from_date: int
    chats_processed: int
    chats_created: int
    chats_updated: int
    messages_loaded: int
    messages_created: int
    messages_updated: int
    errors: Optional[List[str]] = None


class AvitoOAuthCallbackResponse(BaseModel):
    success: bool
    message: str
    channel_id: Optional[int] = None
    cashbox_id: Optional[int] = None
    webhook_registered: Optional[bool] = None
    webhook_url: Optional[str] = None
    webhook_error: Optional[str] = None
