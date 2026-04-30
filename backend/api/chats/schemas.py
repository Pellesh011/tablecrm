from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, validator

S3_CHAT_FILE_SAVE_FORMAT = "{prefix}/chats_files/{cashbox_id}/{channel_id}/{date_path}/message_{message_id}_{message_hex}.{extension}"
TELEGRAM_FILE_ID_PREFIX = "telegram_file_id:"


class ChannelCreate(BaseModel):
    name: str
    type: str
    description: Optional[str] = None
    svg_icon: Optional[str] = None
    tags: Optional[dict] = None
    api_config_name: Optional[str] = None


class ChannelUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    svg_icon: Optional[str] = None
    tags: Optional[dict] = None
    api_config_name: Optional[str] = None
    is_active: Optional[bool] = None


class ChannelResponse(BaseModel):
    id: int
    name: str
    type: str
    description: Optional[str]
    svg_icon: Optional[str]
    tags: Optional[dict]
    api_config_name: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime


class ChatCreate(BaseModel):
    channel_id: int
    external_chat_id: str
    phone: Optional[str] = None
    name: Optional[str] = None
    assigned_operator_id: Optional[int] = None


class ContactInfo(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    avatar: Optional[str] = None
    contragent_id: Optional[int] = None
    last_activity: Optional[int] = None


class ChatResponse(BaseModel):
    id: int
    channel_id: int
    chat_contact_id: Optional[int] = None
    cashbox_id: int
    external_chat_id: str
    status: str
    assigned_operator_id: Optional[int] = None
    first_message_time: Optional[datetime] = None
    first_response_time_seconds: Optional[int] = None
    last_message_time: Optional[datetime] = None
    last_response_time_seconds: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    last_message_preview: Optional[str] = None
    unread_count: int = 0
    related_chats_count: int = 1
    channel_name: Optional[str] = None
    channel_icon: Optional[str] = None
    channel_type: Optional[str] = None
    name: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    contact: Optional[ContactInfo] = None
    from_qr: bool = False
    qr_page_id: Optional[int] = None


class TelegramButton(BaseModel):
    text: str
    callback_data: Optional[str] = None
    url: Optional[str] = None
    request_contact: Optional[bool] = None
    request_location: Optional[bool] = None

    @validator("url", always=True)
    def validate_action(cls, v, values):
        if (
            not v
            and not values.get("callback_data")
            and not values.get("request_contact")
            and not values.get("request_location")
        ):
            raise ValueError(
                "One of callback_data, url, request_contact, request_location is required"
            )
        return v


class MessageCreate(BaseModel):
    chat_id: int
    sender_type: str
    content: str
    message_type: str = "TEXT"
    status: str = "SENT"
    image_url: Optional[str] = None
    source: Optional[str] = None
    files: Optional[List[str]] = None
    buttons: Optional[List[List[TelegramButton]]] = None
    buttons_type: Optional[str] = "inline"
    buttons_resize: Optional[bool] = True
    buttons_one_time: Optional[bool] = False


class MessageResponse(BaseModel):
    id: int
    chat_id: int
    sender_type: str
    message_type: str
    content: str
    external_message_id: Optional[str]
    status: str
    created_at: datetime
    updated_at: datetime
    sender_avatar: Optional[str] = None
    source: Optional[str] = None
    image_url: Optional[str] = None
    file_url: Optional[str] = None

    @validator("created_at", "updated_at", pre=True)
    def convert_datetime(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                return v.replace(tzinfo=None)
            return v
        return v


class ChatAttachmentResponse(BaseModel):
    message_id: int
    message_type: str
    url: str
    created_at: datetime
    preview_text: Optional[str] = None

    @validator("created_at", pre=True)
    def convert_attachment_datetime(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                return v.replace(tzinfo=None)
            return v
        return v


class ChainClientRequest(BaseModel):
    phone: Optional[str] = None
    name: Optional[str] = None


class MessagesList(BaseModel):
    data: List[MessageResponse]
    total: int
    skip: int
    limit: int
    date: Optional[datetime] = None

    @validator("date", pre=True)
    def convert_datetime(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                return v.replace(tzinfo=None)
            return v
        return v


class ManagerInChat(BaseModel):
    user_id: int
    user_type: str
    connected_at: str


class ManagersInChatResponse(BaseModel):
    chat_id: int
    managers: List[ManagerInChat]
    total: int
