import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from api.chats import crud
from api.chats.auth import get_current_user, get_current_user_owner
from api.chats.producer import chat_producer
from api.chats.schemas import (
    S3_CHAT_FILE_SAVE_FORMAT,
    TELEGRAM_FILE_ID_PREFIX,
    ChainClientRequest,
    ChannelCreate,
    ChannelResponse,
    ChannelUpdate,
    ChatAttachmentResponse,
    ChatCreate,
    ChatResponse,
    ManagerInChat,
    ManagersInChatResponse,
    MessageCreate,
    MessageResponse,
    MessagesList,
)
from api.chats.telegram.telegram_handler import refresh_telegram_avatar
from api.chats.websocket import cashbox_manager, chat_manager
from common.utils.url_helper import (
    get_app_url_for_environment,
)
from database.db import (
    MessageType,
    channels,
    chat_contacts,
    chat_messages,
    chats,
    database,
    pictures,
)
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse, Response
from sqlalchemy import and_, select, update

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chats", tags=["chats"])


def _build_telegram_inline_keyboard(buttons):
    if not buttons:
        return None

    keyboard = []
    for row in buttons:
        row_buttons = []
        for button in row:
            data = {"text": button.text}
            if button.url:
                data["url"] = button.url
            else:
                data["callback_data"] = button.callback_data or button.text
            row_buttons.append(data)
        if row_buttons:
            keyboard.append(row_buttons)

    if not keyboard:
        return None

    return {"inline_keyboard": keyboard}


def _build_telegram_reply_keyboard(
    buttons, resize_keyboard=True, one_time_keyboard=False
):
    if not buttons:
        return None

    keyboard = []
    for row in buttons:
        row_buttons = []
        for button in row:
            data = {"text": button.text}
            if button.request_contact:
                data["request_contact"] = True
            if button.request_location:
                data["request_location"] = True
            row_buttons.append(data)
        if row_buttons:
            keyboard.append(row_buttons)

    if not keyboard:
        return None

    return {
        "keyboard": keyboard,
        "resize_keyboard": bool(resize_keyboard),
        "one_time_keyboard": bool(one_time_keyboard),
    }


def _parse_data_url(data_url: str):
    import base64

    if (
        not isinstance(data_url, str)
        or not data_url.startswith("data:")
        or "," not in data_url
    ):
        return None
    try:
        header, encoded = data_url.split(",", 1)
        encoded = encoded.strip().replace("\n", "").replace("\r", "")
        if not encoded:
            return None
        content_type = header.split(";")[0].split(":")[1] if ":" in header else None
        decoded = base64.b64decode(encoded, validate=False)
        if not decoded:
            return None
        return (decoded, content_type)
    except Exception:
        return None


def _is_placeholder_content(content: Optional[str]) -> bool:
    if not content:
        return False
    normalized = content.strip().lower()
    if normalized.startswith("[file"):
        return True
    if normalized.startswith("[doc"):
        return True
    if normalized.startswith("[document"):
        return True
    if normalized.startswith("[image"):
        return True
    return normalized in {
        "[photo]",
        "[document]",
        "[doc]",
        "[file]",
        "[image]",
        "[video]",
        "[voice]",
        "[message]",
        "photo",
        "file",
        "doc",
        "image",
        "document",
        "video",
        "voice",
        "message",
        "документ",
        "фото",
        "видео",
        "голосовое",
        "сообщение",
    }


async def _refresh_telegram_avatar_for_chat(chat: dict) -> None:
    if not chat or chat.get("channel_type") != "TELEGRAM":
        return
    contact = chat.get("contact") or {}
    if contact.get("avatar"):
        return
    contact_id = chat.get("chat_contact_id")
    if not contact_id:
        return

    contact_row = await database.fetch_one(
        chat_contacts.select().where(chat_contacts.c.id == contact_id)
    )
    if not contact_row:
        return

    external_contact_id = contact_row.get("external_contact_id")
    if not external_contact_id:
        external_contact_id = chat.get("external_chat_id")
    if not external_contact_id:
        return

    avatar_url = await refresh_telegram_avatar(
        channel_id=chat.get("channel_id"),
        cashbox_id=chat.get("cashbox_id"),
        external_contact_id=external_contact_id,
    )
    if not avatar_url:
        return

    await database.execute(
        chat_contacts.update()
        .where(chat_contacts.c.id == contact_id)
        .values(avatar=avatar_url, updated_at=datetime.utcnow())
    )
    contact["avatar"] = avatar_url
    chat["contact"] = contact


def _normalize_telegram_file_url(file_url: Optional[str]) -> Optional[str]:
    if not file_url:
        return file_url

    app_url = get_app_url_for_environment()
    if not app_url:
        return file_url

    if not app_url.startswith("http"):
        app_url = f"https://{app_url}"

    scheme, host = app_url.split("://", 1)
    if file_url.startswith("http://") or file_url.startswith("https://"):
        if "api.telegram.org/" in file_url:
            return file_url
        if host in file_url:
            normalized = file_url.split(host, 1)[-1].lstrip("/")
            if normalized.startswith(host):
                normalized = normalized.split(host, 1)[-1].lstrip("/")
            if normalized.startswith("api/v1/photos/"):
                return f"{scheme}://{host}/{normalized}"
            if normalized.startswith("photos/") or normalized.startswith(
                "chats_files/"
            ):
                return f"{scheme}://{host}/api/v1/photos/{normalized}"
        return file_url

    normalized = file_url.lstrip("/")
    if normalized.startswith(host):
        return f"{scheme}://{normalized}"
    if normalized.startswith("api/v1/photos/"):
        return f"{app_url.rstrip('/')}/{normalized}"
    if normalized.startswith("photos/") or normalized.startswith("chats_files/"):
        return f"{app_url.rstrip('/')}/api/v1/photos/{normalized}"
    return f"{app_url.rstrip('/')}/{normalized}"


def _get_app_url_base() -> str:
    app_url = (get_app_url_for_environment() or "").rstrip("/")
    if app_url and not app_url.startswith("http"):
        app_url = f"https://{app_url}"
    return app_url


def _build_telegram_proxy_url(
    message_id: Optional[int], token: Optional[str], app_url: str
) -> Optional[str]:
    if not message_id or not token or not app_url:
        return None
    return (
        f"{app_url}/chats/messages/{message_id}/telegram-image"
        f"?token={quote(str(token), safe='')}"
    )


def _normalize_message_type_value(raw_message_type: Any) -> str:
    normalized = getattr(raw_message_type, "value", raw_message_type) or "TEXT"
    return str(normalized).upper()


async def _load_message_picture_urls(message_ids: List[int]) -> Dict[int, List[str]]:
    if not message_ids:
        return {}

    query = (
        select(pictures)
        .where(
            pictures.c.entity == "messages",
            pictures.c.entity_id.in_(message_ids),
            pictures.c.is_deleted.is_not(True),
        )
        .order_by(pictures.c.created_at.asc())
    )
    rows = await database.fetch_all(query)

    picture_urls: Dict[int, List[str]] = {}
    for row in rows:
        raw_url = row.get("url")
        if not raw_url:
            continue
        picture_urls.setdefault(row["entity_id"], []).append(raw_url)
    return picture_urls


async def _get_chat_files_context(chat_id: int) -> Optional[Dict[str, Any]]:
    query = (
        select(
            [
                chats.c.id,
                chats.c.cashbox_id,
                chats.c.channel_id,
                channels.c.type.label("channel_type"),
            ]
        )
        .select_from(chats.join(channels, chats.c.channel_id == channels.c.id))
        .where(chats.c.id == chat_id)
        .limit(1)
    )
    row = await database.fetch_one(query)
    return dict(row) if row else None


async def _get_chat_messages_context(chat_id: int) -> Optional[Dict[str, Any]]:
    query = (
        select(
            [
                chats.c.id,
                chats.c.cashbox_id,
                chats.c.channel_id,
                chats.c.external_chat_id,
                chats.c.chat_contact_id,
                channels.c.type.label("channel_type"),
                chat_contacts.c.avatar.label("contact_avatar"),
            ]
        )
        .select_from(
            chats.join(channels, chats.c.channel_id == channels.c.id).outerjoin(
                chat_contacts, chats.c.chat_contact_id == chat_contacts.c.id
            )
        )
        .where(chats.c.id == chat_id)
        .limit(1)
    )
    row = await database.fetch_one(query)
    if not row:
        return None

    chat = dict(row)
    contact_avatar = chat.pop("contact_avatar", None)
    chat["contact"] = {"avatar": contact_avatar} if contact_avatar else None
    return chat


async def _get_chat_attachment_messages(chat_id: int, limit: int = 1000):
    attachment_types = [MessageType.IMAGE, *crud.MEDIA_FILE_TYPES]
    query = (
        chat_messages.select()
        .where(
            and_(
                chat_messages.c.chat_id == chat_id,
                chat_messages.c.message_type.in_(attachment_types),
            )
        )
        .order_by(chat_messages.c.created_at.desc())
        .limit(limit)
    )
    return await database.fetch_all(query)


def _resolve_message_media(
    message: Dict[str, Any],
    channel_type: Optional[str],
    token: Optional[str],
    picture_urls: Optional[List[str]] = None,
) -> Dict[str, Optional[str]]:
    message_type = _normalize_message_type_value(message.get("message_type"))
    message_id = message.get("id")
    channel_type_upper = str(channel_type or "").upper()
    app_url = _get_app_url_base()

    raw_url = next((url for url in picture_urls or [] if url), None)
    resolved_url = None

    if raw_url:
        if raw_url.startswith(TELEGRAM_FILE_ID_PREFIX):
            if message_type == MessageType.IMAGE and channel_type_upper == "TELEGRAM":
                resolved_url = _build_telegram_proxy_url(message_id, token, app_url)
        else:
            resolved_url = _normalize_telegram_file_url(raw_url)

    if not resolved_url:
        direct_url = crud.extract_message_media_url_from_content(message.get("content"))
        if direct_url:
            resolved_url = _normalize_telegram_file_url(direct_url)

    if (
        not resolved_url
        and message_type == MessageType.IMAGE
        and channel_type_upper == "TELEGRAM"
    ):
        resolved_url = _build_telegram_proxy_url(message_id, token, app_url)

    if message_type == MessageType.IMAGE:
        return {"image_url": resolved_url, "file_url": resolved_url}

    if message_type in crud.MEDIA_FILE_TYPES:
        return {"image_url": None, "file_url": resolved_url}

    return {"image_url": None, "file_url": None}


def _build_attachment_item(
    message: Dict[str, Any],
    channel_type: Optional[str],
    token: Optional[str],
    picture_urls: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    message_type = _normalize_message_type_value(message.get("message_type"))
    if message_type not in {MessageType.IMAGE, *crud.MEDIA_FILE_TYPES}:
        return None

    media = _resolve_message_media(
        message=message,
        channel_type=channel_type,
        token=token,
        picture_urls=picture_urls,
    )
    url = (
        media.get("image_url")
        if message_type == MessageType.IMAGE
        else media.get("file_url")
    )
    if not url:
        return None

    preview_text = (message.get("content") or "").strip() or None
    if preview_text and _is_placeholder_content(preview_text):
        preview_text = None

    return {
        "message_id": message.get("id"),
        "message_type": message_type,
        "url": url,
        "created_at": message.get("created_at"),
        "preview_text": preview_text,
    }


@router.post("/channels/", response_model=ChannelResponse)
async def create_channel(
    token: str, channel: ChannelCreate, user=Depends(get_current_user_owner)
):
    """Create a new channel (owner only)"""
    return await crud.create_channel(
        name=channel.name,
        type=channel.type,
        description=channel.description,
        svg_icon=channel.svg_icon,
        tags=channel.tags,
        api_config_name=channel.api_config_name,
    )


@router.get("/channels/{channel_id}", response_model=ChannelResponse)
async def get_channel(channel_id: int, token: str, user=Depends(get_current_user)):
    channel = await crud.get_channel_by_id_and_cashbox(channel_id, user.cashbox_id)
    if not channel:
        raise HTTPException(
            status_code=404, detail="Channel not found or access denied"
        )
    return channel


@router.get("/channels/", response_model=list)
async def get_channels(
    token: str,
    skip: int = 0,
    limit: int = 100,
    channel_type: Optional[str] = None,
    user=Depends(get_current_user),
):
    return await crud.get_all_channels_by_cashbox(user.cashbox_id, channel_type)


@router.put("/channels/{channel_id}", response_model=ChannelResponse)
async def update_channel(
    channel_id: int,
    token: str,
    channel: ChannelUpdate,
    user=Depends(get_current_user_owner),
):
    """Update channel (owner only)"""
    return await crud.update_channel(channel_id, **channel.dict(exclude_unset=True))


@router.delete("/channels/{channel_id}")
async def delete_channel(
    channel_id: int, token: str, user=Depends(get_current_user_owner)
):
    """Delete channel (owner only, soft-delete)"""
    return await crud.delete_channel(channel_id)


@router.post("/chats/", response_model=ChatResponse)
async def create_chat(token: str, chat: ChatCreate, user=Depends(get_current_user)):
    """Create a new chat (cashbox_id from token)"""
    return await crud.create_chat(
        channel_id=chat.channel_id,
        cashbox_id=user.cashbox_id,
        external_chat_id=chat.external_chat_id,
        assigned_operator_id=chat.assigned_operator_id,
        external_chat_id_for_contact=chat.external_chat_id,
        phone=chat.phone,
        name=chat.name,
    )


@router.get("/chats/{chat_id}", response_model=ChatResponse)
async def get_chat(chat_id: int, token: str, user=Depends(get_current_user)):
    """Get chat by ID (must belong to user's cashbox)"""
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    try:
        await _refresh_telegram_avatar_for_chat(chat)
    except Exception as exc:
        logger.warning(
            "Failed to refresh Telegram avatar for chat %s: %s", chat_id, exc
        )

    return chat


@router.get("/chats/", response_model=list)
async def get_chats(
    token: str,
    channel_id: Optional[int] = None,
    contragent_id: Optional[int] = None,
    status: Optional[str] = None,
    is_qr: Optional[bool] = Query(None, description="Фильтр по регистрации через QR"),
    search: Optional[str] = Query(
        None, description="Поиск по имени контакта или названию объявления"
    ),
    created_from: Optional[datetime] = Query(
        None, description="Фильтр: дата создания от (ISO 8601)"
    ),
    created_to: Optional[datetime] = Query(
        None, description="Фильтр: дата создания до (ISO 8601)"
    ),
    updated_from: Optional[datetime] = Query(
        None, description="Фильтр: дата обновления от (ISO 8601)"
    ),
    updated_to: Optional[datetime] = Query(
        None, description="Фильтр: дата обновления до (ISO 8601)"
    ),
    sort_by: Optional[str] = Query(
        None,
        description="Сортировка по полю: created_at, updated_at, last_message_time, name",
    ),
    sort_order: Optional[str] = Query(
        "desc", description="Порядок сортировки: asc или desc"
    ),
    skip: int = 0,
    limit: int = 100,
    user=Depends(get_current_user),
):
    chats = await crud.get_chats(
        cashbox_id=user.cashbox_id,
        channel_id=channel_id,
        contragent_id=contragent_id,
        status=status,
        search=search,
        created_from=created_from,
        created_to=created_to,
        updated_from=updated_from,
        updated_to=updated_to,
        sort_by=sort_by,
        sort_order=sort_order,
        skip=skip,
        limit=limit,
        is_qr=is_qr,
    )
    return chats


@router.post("/messages/", response_model=MessageResponse)
async def create_message(
    token: str, message: MessageCreate, user=Depends(get_current_user)
):
    """Create a new message"""
    chat = await crud.get_chat(message.chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    has_image = message.message_type == "IMAGE" and message.image_url
    has_text = message.content and message.content.strip()

    if has_image and has_text:
        db_message_image = await crud.create_message_and_update_chat(
            chat_id=message.chat_id,
            sender_type=message.sender_type,
            content="",
            message_type="IMAGE",
            external_message_id=None,
            status=message.status,
            source=message.source or "api",
        )

        db_message_text = await crud.create_message_and_update_chat(
            chat_id=message.chat_id,
            sender_type=message.sender_type,
            content=message.content,
            message_type="TEXT",
            external_message_id=None,
            status=message.status,
            source=message.source or "api",
        )

        db_message = db_message_image
    else:
        db_message = await crud.create_message_and_update_chat(
            chat_id=message.chat_id,
            sender_type=message.sender_type,
            content=message.content,
            message_type=message.message_type,
            external_message_id=None,
            status=message.status,
            source=message.source or "api",
        )
        db_message_text = None

    if message.sender_type == "OPERATOR":
        try:
            channel = await crud.get_channel(chat["channel_id"])

            if channel and channel["type"] == "AVITO" and chat.get("external_chat_id"):
                from api.chats.avito.avito_factory import (
                    create_avito_client,
                    save_token_callback,
                )

                client = await create_avito_client(
                    channel_id=channel["id"],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel["id"], user.cashbox_id, token_data
                    ),
                )

                if client:
                    try:
                        image_id = None
                        image_url_for_db = None
                        image_data = None
                        filename = "image.jpg"

                        if message.image_url and (
                            message.message_type == "IMAGE" or (has_image and has_text)
                        ):
                            try:
                                import base64
                                import io
                                from os import environ
                                from uuid import uuid4

                                import aioboto3
                                import aiohttp

                                image_url = message.image_url
                                content_type = None

                                if image_url.startswith("data:"):
                                    try:
                                        header, encoded = image_url.split(",", 1)
                                        content_type = header.split(";")[0].split(":")[
                                            1
                                        ]
                                        image_data = base64.b64decode(encoded)

                                        if "png" in content_type:
                                            filename = "image.png"
                                        elif "gif" in content_type:
                                            filename = "image.gif"
                                        elif "webp" in content_type:
                                            filename = "image.webp"
                                        elif (
                                            "jpeg" in content_type
                                            or "jpg" in content_type
                                        ):
                                            filename = "image.jpg"
                                        else:
                                            filename = "image.jpg"
                                    except Exception as e:
                                        logger.error(f"Failed to parse data URL: {e}")
                                        image_data = None

                                if image_data is None:
                                    if (
                                        "google.com/imgres" in image_url
                                        or "imgurl=" in image_url
                                    ):
                                        from urllib.parse import (
                                            parse_qs,
                                            unquote,
                                            urlparse,
                                        )

                                        try:
                                            parsed = urlparse(image_url)
                                            params = parse_qs(parsed.query)
                                            if "imgurl" in params:
                                                real_url = unquote(params["imgurl"][0])
                                                image_url = real_url
                                        except Exception:
                                            pass

                                    headers = {}
                                    if "avito" in image_url.lower():
                                        headers["Authorization"] = (
                                            f"Bearer {client.access_token}"
                                        )

                                    connector = aiohttp.TCPConnector(ssl=False)
                                    async with aiohttp.ClientSession(
                                        connector=connector
                                    ) as session:
                                        async with session.get(
                                            image_url, headers=headers
                                        ) as img_response:
                                            if img_response.status == 200:
                                                content_type = img_response.headers.get(
                                                    "Content-Type", ""
                                                )

                                                if not content_type.startswith(
                                                    "image/"
                                                ):
                                                    image_data = None
                                                else:
                                                    image_data = (
                                                        await img_response.read()
                                                    )

                                                    if len(image_data) == 0:
                                                        image_data = None
                                                    elif (
                                                        len(image_data)
                                                        > 24 * 1024 * 1024
                                                    ):
                                                        image_data = None
                                                    else:
                                                        filename = (
                                                            image_url.split("/")[
                                                                -1
                                                            ].split("?")[0]
                                                            or "image.jpg"
                                                        )
                                                        if "." not in filename:
                                                            if "png" in content_type:
                                                                filename = "image.png"
                                                            elif "gif" in content_type:
                                                                filename = "image.gif"
                                                            elif "webp" in content_type:
                                                                filename = "image.webp"
                                                            else:
                                                                filename = "image.jpg"

                                if (
                                    image_data
                                    and len(image_data) > 0
                                    and len(image_data) <= 24 * 1024 * 1024
                                ):
                                    try:
                                        s3_session = aioboto3.Session()
                                        s3_data = {
                                            "service_name": "s3",
                                            "endpoint_url": environ.get("S3_URL"),
                                            "aws_access_key_id": environ.get(
                                                "S3_ACCESS"
                                            ),
                                            "aws_secret_access_key": environ.get(
                                                "S3_SECRET"
                                            ),
                                        }
                                        bucket_name = "5075293c-docs_generated"

                                        file_link = S3_CHAT_FILE_SAVE_FORMAT.format(
                                            prefix="photos",
                                            cashbox_id=chat["cashbox_id"],
                                            channel_id=chat["channel_id"],
                                            date_path=datetime.utcnow().strftime(
                                                "%Y/%m/%d"
                                            ),
                                            message_id=db_message["id"],
                                            message_hex=uuid4().hex[:8],
                                            extension=filename.split(".")[-1],
                                        )

                                        async with s3_session.client(**s3_data) as s3:
                                            await s3.upload_fileobj(
                                                io.BytesIO(image_data),
                                                bucket_name,
                                                file_link,
                                            )

                                        image_url_for_db = file_link
                                    except Exception as e:
                                        logger.error(f"Failed to save file to S3: {e}")

                                if (
                                    message.message_type == "IMAGE"
                                    and image_data
                                    and len(image_data) > 0
                                    and len(image_data) <= 24 * 1024 * 1024
                                ):
                                    try:
                                        upload_result = await client.upload_image(
                                            image_data, filename
                                        )

                                        if upload_result:
                                            if isinstance(upload_result, tuple):
                                                image_id, avito_image_url = (
                                                    upload_result
                                                )
                                                if avito_image_url:
                                                    image_url_for_db = avito_image_url
                                            else:
                                                image_id = upload_result
                                        else:
                                            image_id = None
                                    except Exception as e:
                                        logger.error(
                                            f"Failed to upload image to Avito: {e}"
                                        )
                                        image_id = None

                            except Exception as e:
                                logger.error(f"Failed to process image: {e}")
                                image_id = None

                        if (
                            message.message_type == "IMAGE" or (has_image and has_text)
                        ) and image_url_for_db:
                            try:
                                from database.db import pictures

                                await database.execute(
                                    pictures.insert().values(
                                        entity="messages",
                                        entity_id=db_message["id"],
                                        url=image_url_for_db,
                                        is_main=False,
                                        is_deleted=False,
                                        owner=user.id,
                                        cashbox=user.cashbox_id,
                                        size=len(image_data) if image_data else None,
                                    )
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to save file for message {db_message['id']}: {e}"
                                )

                        if has_image and has_text and db_message_text:
                            if image_id:
                                try:
                                    avito_message_image = await client.send_message(
                                        chat_id=chat["external_chat_id"],
                                        text=None,
                                        image_id=image_id,
                                    )

                                    external_message_id = avito_message_image.get("id")
                                    if external_message_id:
                                        await crud.update_message(
                                            db_message["id"],
                                            external_message_id=external_message_id,
                                            status="DELIVERED",
                                        )
                                except Exception as e:
                                    logger.error(
                                        f"Failed to send IMAGE message to Avito: {e}"
                                    )
                                    await crud.update_message(
                                        db_message["id"], status="FAILED"
                                    )

                            try:
                                avito_message_text = await client.send_message(
                                    chat_id=chat["external_chat_id"],
                                    text=message.content,
                                    image_id=None,
                                )

                                external_message_id = avito_message_text.get("id")
                                if external_message_id:
                                    await crud.update_message(
                                        db_message_text["id"],
                                        external_message_id=external_message_id,
                                        status="DELIVERED",
                                    )
                            except Exception as e:
                                logger.error(
                                    f"Failed to send TEXT message to Avito: {e}"
                                )
                                await crud.update_message(
                                    db_message_text["id"], status="FAILED"
                                )
                        else:
                            send_image = message.message_type == "IMAGE" and image_id
                            send_text = message.content and message.content.strip()

                            if send_image or send_text:
                                try:
                                    avito_message = await client.send_message(
                                        chat_id=chat["external_chat_id"],
                                        text=message.content if send_text else None,
                                        image_id=image_id if send_image else None,
                                    )

                                    external_message_id = avito_message.get("id")
                                    if external_message_id:
                                        await crud.update_message(
                                            db_message["id"],
                                            external_message_id=external_message_id,
                                            status="DELIVERED",
                                        )
                                except Exception as e:
                                    logger.error(
                                        f"Failed to send message to Avito: {e}"
                                    )
                                    await crud.update_message(
                                        db_message["id"], status="FAILED"
                                    )
                            else:
                                logger.warning(
                                    "Cannot send message: no image_id and no text content"
                                )
                                await crud.update_message(
                                    db_message["id"], status="FAILED"
                                )
                    except Exception as e:
                        logger.error(f"Failed to send message to Avito: {e}")
                        try:
                            await crud.update_message(db_message["id"], status="FAILED")
                        except Exception:
                            pass
                else:
                    logger.warning(
                        f"Could not create Avito client for channel {channel['id']}, cashbox {user.cashbox_id}"
                    )
            elif (
                channel
                and channel["type"] == "TELEGRAM"
                and chat.get("external_chat_id")
            ):
                try:
                    from api.chats.avito.avito_factory import _decrypt_credential
                    from api.chats.telegram.telegram_client import (
                        send_document,
                        send_media_group,
                        send_message,
                        send_photo,
                        send_video,
                    )
                    from database.db import channel_credentials, pictures

                    creds = await database.fetch_one(
                        channel_credentials.select().where(
                            (channel_credentials.c.channel_id == channel["id"])
                            & (channel_credentials.c.cashbox_id == user.cashbox_id)
                            & (channel_credentials.c.is_active.is_(True))
                        )
                    )

                    if not creds:
                        logger.warning(
                            f"No Telegram credentials for channel {channel['id']}, cashbox {user.cashbox_id}"
                        )
                        return db_message

                    bot_token = _decrypt_credential(creds["api_key"])
                    chat_id = chat["external_chat_id"]
                    if message.buttons_type == "reply":
                        keyboard = _build_telegram_reply_keyboard(
                            message.buttons,
                            resize_keyboard=message.buttons_resize,
                            one_time_keyboard=message.buttons_one_time,
                        )
                    else:
                        keyboard = _build_telegram_inline_keyboard(message.buttons)

                    file_urls = message.files or []
                    if message.image_url:
                        file_urls = [message.image_url] + file_urls
                    elif (
                        message.message_type == "IMAGE"
                        and message.content
                        and message.content.strip()
                    ):
                        img = message.content.strip()
                        if img.startswith("data:"):
                            file_urls = [img] + file_urls
                        else:
                            file_urls = [f"data:image/jpeg;base64,{img}"] + file_urls

                    normalized_urls = []
                    for url in file_urls:
                        if isinstance(url, str) and url.startswith("data:"):
                            normalized_urls.append(url)
                        else:
                            normalized_urls.append(_normalize_telegram_file_url(url))
                    file_urls = normalized_urls

                    stored_urls = []
                    data_url_bytes = None
                    data_url_content_type = None

                    if len(file_urls) == 1 and file_urls[0].startswith("data:"):
                        parsed = _parse_data_url(file_urls[0])
                        if parsed:
                            data_url_bytes, data_url_content_type = parsed
                            try:
                                import io
                                import os
                                from uuid import uuid4

                                import aioboto3

                                extension = "bin"
                                if data_url_content_type:
                                    if "jpeg" in data_url_content_type:
                                        extension = "jpg"
                                    elif "png" in data_url_content_type:
                                        extension = "png"
                                    elif "gif" in data_url_content_type:
                                        extension = "gif"
                                    elif "pdf" in data_url_content_type:
                                        extension = "pdf"
                                s3_session = aioboto3.Session()
                                s3_data = {
                                    "service_name": "s3",
                                    "endpoint_url": os.environ.get("S3_URL"),
                                    "aws_access_key_id": os.environ.get("S3_ACCESS"),
                                    "aws_secret_access_key": os.environ.get(
                                        "S3_SECRET"
                                    ),
                                }
                                bucket_name = "5075293c-docs_generated"

                                file_link = S3_CHAT_FILE_SAVE_FORMAT.format(
                                    prefix="photos",
                                    cashbox_id=chat["cashbox_id"],
                                    channel_id=chat["channel_id"],
                                    date_path=datetime.utcnow().strftime("%Y/%m/%d"),
                                    message_id=db_message["id"],
                                    message_hex=uuid4().hex[:8],
                                    extension=extension,
                                )

                                async with s3_session.client(**s3_data) as s3:
                                    await s3.upload_fileobj(
                                        io.BytesIO(data_url_bytes),
                                        bucket_name,
                                        file_link,
                                    )
                                stored_urls.append(file_link)
                            except Exception:
                                pass
                        else:
                            pass
                    else:
                        stored_urls = [
                            u
                            for u in file_urls
                            if isinstance(u, str) and not u.startswith("data:")
                        ]
                        if not stored_urls:
                            stored_urls = list(file_urls)

                    for url in stored_urls:
                        if isinstance(url, str) and url.startswith("data:"):
                            continue
                        try:
                            await database.execute(
                                pictures.insert().values(
                                    entity="messages",
                                    entity_id=db_message["id"],
                                    url=url,
                                    is_main=False,
                                    is_deleted=False,
                                    owner=user.id,
                                    cashbox=user.cashbox_id,
                                )
                            )
                        except Exception:
                            pass

                    send_result = None
                    external_id = None
                    send_separate_text = bool(
                        has_image and has_text and db_message_text
                    )
                    telegram_photo_filename = "photo.jpg"
                    if data_url_content_type:
                        if "png" in data_url_content_type:
                            telegram_photo_filename = "photo.png"
                        elif "gif" in data_url_content_type:
                            telegram_photo_filename = "photo.gif"
                        elif "webp" in data_url_content_type:
                            telegram_photo_filename = "photo.webp"

                    if len(file_urls) > 1:
                        media_type = "photo"
                        if message.message_type == "VIDEO":
                            media_type = "video"
                        elif message.message_type == "DOCUMENT":
                            media_type = "document"

                        media_payload = []
                        for idx, url in enumerate(file_urls):
                            media_item = {"type": media_type, "media": url}
                            if idx == 0 and message.content:
                                media_item["caption"] = message.content
                            media_payload.append(media_item)

                        send_result = await send_media_group(
                            bot_token, chat_id, media_payload
                        )

                        if keyboard and send_separate_text and db_message_text:
                            text_result = await send_message(
                                bot_token,
                                chat_id,
                                message.content or " ",
                                reply_markup=keyboard,
                            )
                            if text_result:
                                await crud.update_message(
                                    db_message_text["id"],
                                    external_message_id=str(
                                        text_result.get("message_id")
                                    ),
                                    status="DELIVERED",
                                )

                        if send_result:
                            external_id = str(send_result[0].get("message_id"))
                    else:
                        file_payload = file_urls[0] if file_urls else None
                        if data_url_bytes is not None:
                            file_payload = data_url_bytes
                        if isinstance(file_payload, str) and file_payload.startswith(
                            "data:"
                        ):
                            file_payload = None

                        if send_separate_text and file_payload:
                            send_result = await send_photo(
                                bot_token,
                                chat_id,
                                file_payload,
                                caption=None,
                                filename=telegram_photo_filename,
                            )
                            if send_result:
                                external_id = str(send_result.get("message_id"))

                            text_result = await send_message(
                                bot_token,
                                chat_id,
                                message.content,
                                reply_markup=keyboard,
                            )
                            if text_result and db_message_text:
                                await crud.update_message(
                                    db_message_text["id"],
                                    external_message_id=str(
                                        text_result.get("message_id")
                                    ),
                                    status="DELIVERED",
                                )
                        elif message.message_type == "IMAGE" and file_payload:
                            send_result = await send_photo(
                                bot_token,
                                chat_id,
                                file_payload,
                                caption=None,
                                reply_markup=keyboard,
                                filename=telegram_photo_filename,
                            )
                        elif message.message_type == "VIDEO" and file_payload:
                            send_result = await send_video(
                                bot_token,
                                chat_id,
                                file_payload,
                                caption=message.content,
                                reply_markup=keyboard,
                            )
                        elif message.message_type == "DOCUMENT" and file_payload:
                            send_result = await send_document(
                                bot_token,
                                chat_id,
                                file_payload,
                                caption=message.content,
                                reply_markup=keyboard,
                            )
                        else:
                            send_result = await send_message(
                                bot_token,
                                chat_id,
                                message.content,
                                reply_markup=keyboard,
                            )

                        if send_result:
                            external_id = str(send_result.get("message_id"))

                    if external_id:
                        await crud.update_message(
                            db_message["id"],
                            external_message_id=external_id,
                            status="DELIVERED",
                        )
                except Exception:
                    try:
                        await crud.update_message(db_message["id"], status="FAILED")
                    except Exception:
                        pass
            elif (
                channel
                and channel.get("type") == "MAX"
                and chat.get("external_chat_id")
            ):
                from api.chats.avito.avito_factory import _decrypt_credential
                from api.chats.max.max_handler import send_operator_message
                from database.db import channel_credentials

                creds = await database.fetch_one(
                    channel_credentials.select().where(
                        (channel_credentials.c.channel_id == channel["id"])
                        & (channel_credentials.c.cashbox_id == user.cashbox_id)
                        & (channel_credentials.c.is_active.is_(True))
                    )
                )
                if creds:
                    bot_token = _decrypt_credential(creds["api_key"])

                    file_to_send = None
                    if message.files:
                        file_to_send = message.files[0]
                    elif message.image_url:
                        file_to_send = message.image_url

                    external_id = await send_operator_message(
                        chat=chat,
                        text=(
                            message.content if message.message_type == "TEXT" else None
                        ),
                        image_url=file_to_send,
                        cashbox_id=user.cashbox_id,
                        bot_token=bot_token,
                        files=message.files,
                        message_type=message.message_type,
                    )
                    if external_id:
                        await crud.update_message(
                            db_message["id"],
                            external_message_id=external_id,
                            status="DELIVERED",
                        )
                    else:
                        await crud.update_message(db_message["id"], status="FAILED")
                else:
                    print(f"[HTTP] No credentials for MAX channel {channel['id']}")
        except Exception as e:
            logger.error(f"Error sending message to Avito: {e}")

    try:
        preview_url = None
        if message.image_url:
            preview_url = message.image_url
        elif message.files:
            preview_url = message.files[0]

        if preview_url and isinstance(preview_url, str):
            if not preview_url.startswith("data:"):
                preview_url = _normalize_telegram_file_url(preview_url)
            else:
                preview_url = None

        if _is_placeholder_content(db_message.get("content")) and preview_url:
            await crud.update_message(db_message["id"], content=preview_url)
            db_message["content"] = preview_url

        await chat_producer.send_message(
            chat["id"],
            {
                "message_id": db_message["id"],
                "chat_id": chat["id"],
                "sender_type": db_message["sender_type"],
                "content": db_message.get("content") or "",
                "message_type": db_message.get("message_type") or "TEXT",
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

        if db_message_text:
            await chat_producer.send_message(
                chat["id"],
                {
                    "message_id": db_message_text["id"],
                    "chat_id": chat["id"],
                    "sender_type": db_message_text["sender_type"],
                    "content": db_message_text.get("content") or "",
                    "message_type": db_message_text.get("message_type") or "TEXT",
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )
    except Exception as e:
        logger.warning(f"Failed to publish chat message event: {e}")

    return db_message


@router.get("/messages/{message_id}", response_model=MessageResponse)
async def get_message(message_id: int, token: str, user=Depends(get_current_user)):
    """Get message by ID (must belong to user's cashbox)"""
    message = await crud.get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")

    chat = await crud.get_chat(message["chat_id"])
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return message


@router.get("/messages/{message_id}/telegram-image")
async def get_message_telegram_image(
    message_id: int, token: str, user=Depends(get_current_user)
):
    """Прокси изображения из Telegram для сообщения (когда S3 недоступен)."""
    message = await crud.get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    if message.get("message_type") != "IMAGE":
        raise HTTPException(status_code=404, detail="Not an image message")
    chat = await crud.get_chat(message["chat_id"])
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")
    channel = await crud.get_channel(chat["channel_id"])
    if not channel or channel.get("type") != "TELEGRAM":
        raise HTTPException(status_code=404, detail="Not a Telegram chat")

    query = (
        select(pictures)
        .where(
            pictures.c.entity == "messages",
            pictures.c.entity_id == message_id,
            pictures.c.is_deleted.is_not(True),
        )
        .order_by(pictures.c.created_at.asc())
    )
    rows = await database.fetch_all(query)
    telegram_file_id = None
    stored_file_url = None
    for row in rows:
        url = (row.get("url") or "").strip()
        if not url:
            continue
        if url.startswith(TELEGRAM_FILE_ID_PREFIX):
            if telegram_file_id is None:
                telegram_file_id = url[len(TELEGRAM_FILE_ID_PREFIX) :].strip()
        else:
            stored_file_url = _normalize_telegram_file_url(url)
            break
    if stored_file_url:
        return RedirectResponse(url=stored_file_url, status_code=302)
    if not telegram_file_id:
        raise HTTPException(status_code=404, detail="No Telegram file for this message")

    from api.chats.avito.avito_factory import _decrypt_credential
    from api.chats.telegram.telegram_client import download_file, get_file
    from database.db import channel_credentials

    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel["id"])
            & (channel_credentials.c.cashbox_id == user.cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    if not creds or not creds.get("api_key"):
        raise HTTPException(status_code=503, detail="Channel credentials not found")
    bot_token = _decrypt_credential(creds["api_key"])
    try:
        file_meta = await get_file(bot_token, telegram_file_id)
        file_path = file_meta.get("file_path")
        if not file_path:
            raise HTTPException(status_code=404, detail="File path not found")
        file_bytes = await download_file(bot_token, file_path)
    except Exception as e:
        logger.warning(f"Telegram image proxy failed for message {message_id}: {e}")
        raise HTTPException(
            status_code=502, detail="Failed to fetch image from Telegram"
        )

    media_type = "image/jpeg"
    if ".png" in file_path.lower():
        media_type = "image/png"
    elif ".gif" in file_path.lower():
        media_type = "image/gif"
    elif ".webp" in file_path.lower():
        media_type = "image/webp"
    return Response(content=file_bytes, media_type=media_type)


@router.get("/messages/chat/{chat_id}", response_model=MessagesList)
async def get_chat_messages(
    chat_id: int,
    token: str,
    skip: int = 0,
    limit: int = 100,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    user=Depends(get_current_user),
):
    """Get messages from chat (must belong to user's cashbox)"""
    chat = await _get_chat_messages_context(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    async def mark_avito_chat_as_read_background():
        if not (chat.get("channel_type") == "AVITO" and chat.get("external_chat_id")):
            return
        try:
            from api.chats.avito.avito_factory import (
                create_avito_client,
                save_token_callback,
            )

            client = await create_avito_client(
                channel_id=chat["channel_id"],
                cashbox_id=user.cashbox_id,
                on_token_refresh=lambda token_data: save_token_callback(
                    chat["channel_id"], user.cashbox_id, token_data
                ),
            )

            if client:
                try:
                    await client.mark_chat_as_read(chat["external_chat_id"])

                    update_query = (
                        update(chat_messages)
                        .where(
                            and_(
                                chat_messages.c.chat_id == chat_id,
                                chat_messages.c.sender_type == "CLIENT",
                                chat_messages.c.status != "READ",
                            )
                        )
                        .values(status="READ")
                    )
                    await database.execute(update_query)
                except Exception as e:
                    logger.warning(
                        f"Failed to mark chat {chat['external_chat_id']} as read: {e}"
                    )
        except Exception as e:
            logger.warning(f"Failed to mark Avito chat {chat_id} as read: {e}")

    async def mark_telegram_chat_as_read_background():
        """Пометить сообщения Telegram-чата прочитанными в БД и уведомить фронт (API Telegram не даёт read receipts)."""
        if chat.get("channel_type") != "TELEGRAM":
            return
        try:
            update_query = (
                update(chat_messages)
                .where(
                    and_(
                        chat_messages.c.chat_id == chat_id,
                        chat_messages.c.sender_type == "CLIENT",
                        chat_messages.c.status != "READ",
                    )
                )
                .values(status="READ")
            )
            await database.execute(update_query)
            cashbox_id = chat.get("cashbox_id")
            if cashbox_id:
                try:
                    ws_message = {
                        "type": "chat_message",
                        "event": "message_read",
                        "chat_id": chat_id,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                    await cashbox_manager.broadcast_to_cashbox(cashbox_id, ws_message)
                except Exception as e:
                    logger.warning(
                        "Failed to send WebSocket event for Telegram chat read: %s", e
                    )
        except Exception as e:
            logger.warning("Failed to mark Telegram chat %s as read: %s", chat_id, e)

    if chat.get("channel_type") == "AVITO" and chat.get("external_chat_id"):
        background_tasks.add_task(mark_avito_chat_as_read_background)
    if chat.get("channel_type") == "TELEGRAM":
        background_tasks.add_task(mark_telegram_chat_as_read_background)

    messages = await crud.get_messages(chat_id, skip, limit)
    total = await crud.get_messages_count(chat_id)

    messages_list = []
    if messages:
        client_avatar = (
            chat.get("contact", {}).get("avatar") if chat.get("contact") else None
        )
        operator_avatar = None

        if client_avatar:
            client_avatar = _normalize_telegram_file_url(client_avatar)
        if operator_avatar:
            operator_avatar = _normalize_telegram_file_url(operator_avatar)

        message_ids = [msg["id"] for msg in messages] if messages else []
        message_picture_urls: Dict[int, List[str]] = {}
        if message_ids:
            try:
                message_picture_urls = await _load_message_picture_urls(message_ids)
            except Exception as e:
                logger.warning(f"Failed to load media URLs for messages: {e}")

        for msg in messages:
            msg_dict = dict(msg)
            if msg_dict.get("sender_type") == "CLIENT":
                msg_dict["sender_avatar"] = client_avatar
            elif msg_dict.get("sender_type") == "OPERATOR":
                msg_dict["sender_avatar"] = operator_avatar
            else:
                msg_dict["sender_avatar"] = None

            media = _resolve_message_media(
                message=msg_dict,
                channel_type=chat.get("channel_type"),
                token=token,
                picture_urls=message_picture_urls.get(msg_dict.get("id"), []),
            )
            msg_dict["image_url"] = media.get("image_url")
            msg_dict["file_url"] = media.get("file_url")

            messages_list.append(MessageResponse(**msg_dict))

    result = MessagesList(
        data=messages_list,
        total=total,
        skip=skip,
        limit=limit,
        date=chat.get("last_message_time"),
    )

    return result


@router.delete("/messages/{message_id}")
async def delete_message(message_id: int, token: str, user=Depends(get_current_user)):
    existing_message = await crud.get_message(message_id)
    if not existing_message:
        raise HTTPException(status_code=404, detail="Message not found")

    chat = await crud.get_chat(existing_message["chat_id"])
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    if existing_message.get("external_message_id") and chat.get("external_chat_id"):
        try:
            channel = await crud.get_channel(chat["channel_id"])

            if channel and channel["type"] == "AVITO":
                from api.chats.avito.avito_factory import (
                    create_avito_client,
                    save_token_callback,
                )

                client = await create_avito_client(
                    channel_id=channel["id"],
                    cashbox_id=user.cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel["id"], user.cashbox_id, token_data
                    ),
                )

                if client:
                    try:
                        await client.delete_message(
                            chat_id=chat["external_chat_id"],
                            message_id=existing_message["external_message_id"],
                        )
                    except Exception as e:
                        logger.warning(f"Error deleting message in Avito API: {e}")
        except Exception as e:
            logger.warning(f"Error during Avito message deletion: {e}")

    return await crud.delete_message(message_id)


@router.get("/chats/{chat_id}/files/", response_model=List[ChatAttachmentResponse])
async def get_chat_files(chat_id: int, token: str, user=Depends(get_current_user)):
    chat = await _get_chat_files_context(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    messages = await _get_chat_attachment_messages(chat_id, limit=1000)
    message_ids = [msg["id"] for msg in messages]

    if not message_ids:
        return []

    try:
        message_picture_urls = await _load_message_picture_urls(message_ids)
    except Exception as e:
        logger.warning(f"Failed to load message picture URLs for chat files: {e}")
        message_picture_urls = {}

    attachments: List[ChatAttachmentResponse] = []
    for row in messages:
        message = (
            dict(row._mapping)
            if hasattr(row, "_mapping")
            else {k: row[k] for k in row.keys()}
        )
        attachment = _build_attachment_item(
            message=message,
            channel_type=chat.get("channel_type"),
            token=token,
            picture_urls=message_picture_urls.get(message.get("id"), []),
        )
        if attachment:
            attachments.append(ChatAttachmentResponse(**attachment))

    return attachments


@router.post("/chats/{chat_id}/read")
async def mark_chat_as_read(chat_id: int, token: str, user=Depends(get_current_user)):
    chat = await crud.get_chat(chat_id)
    if not chat or chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=404, detail="Chat not found")

    await database.execute(
        chat_messages.update()
        .where(
            (chat_messages.c.chat_id == chat_id)
            & (chat_messages.c.sender_type == "CLIENT")
            & (chat_messages.c.status != "READ")
        )
        .values(status="READ", updated_at=datetime.utcnow())
    )
    return {"success": True}


@router.post("/chats/{chat_id}/chain_client/", response_model=dict)
async def chain_client_endpoint(
    chat_id: int,
    token: str,
    request: ChainClientRequest,
    message_id: Optional[int] = None,
    user=Depends(get_current_user),
):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return await crud.chain_client(
        chat_id=chat_id, message_id=message_id, phone=request.phone, name=request.name
    )


@router.get("/chats/{chat_id}/managers/", response_model=ManagersInChatResponse)
async def get_managers_in_chat(
    chat_id: int, token: str, user=Depends(get_current_user)
):
    chat = await crud.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if chat["cashbox_id"] != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    connected_users = chat_manager.get_connected_users(chat_id)

    managers = [
        ManagerInChat(
            user_id=user_info["user_id"],
            user_type=user_info["user_type"],
            connected_at=user_info["connected_at"],
        )
        for user_info in connected_users
        if user_info["user_type"] == "OPERATOR"
    ]

    return ManagersInChatResponse(
        chat_id=chat_id, managers=managers, total=len(managers)
    )
