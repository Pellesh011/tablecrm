import io
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

import aioboto3
import aiohttp
from database.db import MessageType, database

from .. import crud
from ..producer import chat_producer
from .avito_types import AvitoWebhook

logger = logging.getLogger(__name__)
_AVITO_AVATAR_CACHE: dict = {}


async def _upload_avito_avatar_to_s3(
    avatar_url: str,
    external_contact_id: str,
    cashbox_id: int,
    channel_id: int,
) -> Optional[str]:
    """
    Скачивает аватар с Avito CDN и кладёт в S3.
    Возвращает публичный путь или None.
    """
    if not avatar_url or not avatar_url.startswith("http"):
        return None

    cached = _AVITO_AVATAR_CACHE.get(avatar_url)
    if cached:
        return cached

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                avatar_url, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return None
                file_bytes = await resp.read()
    except Exception as e:
        logger.warning(f"Avito avatar download failed {avatar_url}: {e}")
        return None

    extension = "jpg"
    if file_bytes[:8].startswith(b"\x89PNG"):
        extension = "png"
    elif file_bytes[:6] in (b"GIF87a", b"GIF89a"):
        extension = "gif"

    s3_data = {
        "service_name": "s3",
        "endpoint_url": os.environ.get("S3_URL"),
        "aws_access_key_id": os.environ.get("S3_ACCESS"),
        "aws_secret_access_key": os.environ.get("S3_SECRET"),
    }
    bucket = "5075293c-docs_generated"
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    file_key = (
        f"chats_files/{cashbox_id}/{channel_id}/{date_path}/"
        f"avatar_{external_contact_id}_{uuid4().hex[:8]}.{extension}"
    )

    try:
        s3_session = aioboto3.Session()
        async with s3_session.client(**s3_data) as s3:
            await s3.upload_fileobj(io.BytesIO(file_bytes), bucket, file_key)
    except Exception as e:
        logger.warning(f"Avito avatar S3 upload failed: {e}")
        return None

    app_url = os.environ.get("APP_URL", "").rstrip("/")
    s3_url = f"{app_url}/api/v1/photos/{file_key}" if app_url else file_key

    _AVITO_AVATAR_CACHE[avatar_url] = s3_url
    logger.info(f"Downloading Avito avatar from {avatar_url}")
    logger.info(f"Uploaded Avito avatar to S3: {file_key}")
    return s3_url


async def _download_remote_media_bytes(media_url: str) -> Optional[bytes]:
    if not media_url or not media_url.startswith("http"):
        return None

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                media_url, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"Avito media download failed {media_url}: status={resp.status}"
                    )
                    return None
                media_bytes = await resp.read()
                return media_bytes or None
    except Exception as e:
        logger.warning(f"Avito media download failed {media_url}: {e}")
        return None


async def _upload_media_bytes_to_s3(
    file_bytes: bytes,
    message_id: int,
    cashbox_id: int,
    channel_id: int,
    extension: str,
) -> Optional[str]:
    if not file_bytes or not message_id or not cashbox_id or not channel_id:
        return None

    s3_data = {
        "service_name": "s3",
        "endpoint_url": os.environ.get("S3_URL"),
        "aws_access_key_id": os.environ.get("S3_ACCESS"),
        "aws_secret_access_key": os.environ.get("S3_SECRET"),
    }
    bucket = "5075293c-docs_generated"
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    normalized_extension = (extension or "bin").lstrip(".")
    file_key = (
        f"chats_files/{cashbox_id}/{channel_id}/{date_path}/"
        f"voice_{message_id}_{uuid4().hex[:8]}.{normalized_extension}"
    )

    try:
        s3_session = aioboto3.Session()
        async with s3_session.client(**s3_data) as s3:
            await s3.upload_fileobj(io.BytesIO(file_bytes), bucket, file_key)
        logger.info(f"Uploaded Avito media to S3: {file_key}")
        return file_key
    except Exception as e:
        logger.warning(f"Avito media S3 upload failed for message {message_id}: {e}")
        return None


def extract_phone_from_text(text: str) -> Optional[str]:
    if not text:
        return None

    phone_patterns = [
        r"\+?7\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}",
        r"8\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}",
        r"\+?7\d{10}",
        r"8\d{10}",
    ]

    for pattern in phone_patterns:
        matches = re.findall(pattern, text)
        if matches:
            phone = re.sub(r"[^\d+]", "", matches[0])
            if phone.startswith("8"):
                phone = "+7" + phone[1:]
            elif phone.startswith("7") and not phone.startswith("+7"):
                phone = "+" + phone
            elif len(phone) == 10:
                phone = "+7" + phone

            if phone.startswith("+7") and len(phone) == 12:
                return phone
            elif len(phone) >= 11:
                return phone

    return None


class AvitoHandler:
    @staticmethod
    def _extract_media_url_candidate(value: Any) -> Optional[str]:
        if isinstance(value, str):
            extracted = crud.extract_message_media_url_from_content(value)
            return extracted.replace("\\u0026", "&") if extracted else None

        if isinstance(value, dict):
            priority_keys = (
                "url",
                "file_url",
                "download_url",
                "voice_url",
                "video_url",
                "preview_url",
                "src",
                "href",
            )
            for key in priority_keys:
                candidate = AvitoHandler._extract_media_url_candidate(value.get(key))
                if candidate:
                    return candidate

            nested_priority_keys = (
                "sizes",
                "image",
                "voice",
                "video",
                "file",
                "document",
                "attachment",
                "content",
                "message",
                "data",
                "value",
                "preview",
            )
            for key in nested_priority_keys:
                if key in value:
                    candidate = AvitoHandler._extract_media_url_candidate(
                        value.get(key)
                    )
                    if candidate:
                        return candidate

            for nested_value in value.values():
                candidate = AvitoHandler._extract_media_url_candidate(nested_value)
                if candidate:
                    return candidate

        if isinstance(value, list):
            for item in value:
                candidate = AvitoHandler._extract_media_url_candidate(item)
                if candidate:
                    return candidate

        return None

    @staticmethod
    def _extract_voice_id(value: Any) -> Optional[str]:
        if isinstance(value, dict):
            voice_id = value.get("voice_id")
            if voice_id:
                return str(voice_id)
            for nested_value in value.values():
                nested_voice_id = AvitoHandler._extract_voice_id(nested_value)
                if nested_voice_id:
                    return nested_voice_id

        if isinstance(value, list):
            for item in value:
                nested_voice_id = AvitoHandler._extract_voice_id(item)
                if nested_voice_id:
                    return nested_voice_id

        return None

    @staticmethod
    def _extract_message_media_file_url(
        content: Any, message_type: Optional[str]
    ) -> Optional[str]:
        normalized_type = (message_type or "").lower()
        if not content:
            return None

        candidates = []
        if isinstance(content, dict):
            if normalized_type in content:
                candidates.append(content.get(normalized_type))

            if normalized_type == "document" and "file" in content:
                candidates.append(content.get("file"))
            if normalized_type == "file" and "document" in content:
                candidates.append(content.get("document"))

            candidates.append(content)
        else:
            candidates.append(content)

        for candidate in candidates:
            media_url = AvitoHandler._extract_media_url_candidate(candidate)
            if media_url:
                return media_url

        return None

    @staticmethod
    async def _enrich_media_message_from_avito(
        avito_channel: Optional[Dict[str, Any]],
        cashbox_id: int,
        chat_id_external: str,
        message_id: str,
        message_type: str,
        message_content: Dict[str, Any],
        message_text: str,
    ) -> tuple[Dict[str, Any], str]:
        if not (
            avito_channel
            and chat_id_external
            and message_id
            and message_type in {"image", "voice", "video", "file", "document"}
        ):
            return message_content, message_text

        try:
            from api.chats.avito.avito_factory import (
                create_avito_client,
                save_token_callback,
            )

            client = await create_avito_client(
                channel_id=avito_channel["id"],
                cashbox_id=cashbox_id,
                on_token_refresh=lambda token_data: save_token_callback(
                    avito_channel["id"], cashbox_id, token_data
                ),
            )
            if not client:
                return message_content, message_text

            recent_messages = await client.get_messages(chat_id_external, limit=50)
            for recent_message in recent_messages:
                if str(recent_message.get("id") or "") != str(message_id):
                    continue

                recent_type = recent_message.get("type") or message_type
                recent_content = recent_message.get("content") or {}
                enriched_content, enriched_text = AvitoHandler._extract_message_content(
                    recent_content, recent_type
                )
                return enriched_content, enriched_text or message_text
        except Exception as e:
            logger.warning(
                f"Failed to enrich Avito media message {message_id} in chat {chat_id_external}: {e}"
            )

        return message_content, message_text

    @staticmethod
    async def handle_message_event(
        webhook: AvitoWebhook,
        cashbox_id: int,
        channel_id: Optional[int] = None,
        raw_payload_value: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value
            chat_id_external = (
                (payload.chat_id or "").strip() if payload.chat_id else ""
            )
            if (
                not chat_id_external
                and raw_payload_value
                and isinstance(raw_payload_value, dict)
            ):
                raw_chat = (
                    raw_payload_value.get("chat_id")
                    or raw_payload_value.get("chatId")
                    or raw_payload_value.get("chat")
                )
                if raw_chat is not None:
                    if isinstance(raw_chat, dict):
                        chat_id_external = str(raw_chat.get("id") or "").strip()
                    else:
                        chat_id_external = str(raw_chat).strip()

            author_id = payload.author_id
            user_id = payload.user_id
            message_id = payload.id or ""
            message_type = payload.type or "text"
            logger.warning(
                f"[Avito] Message event: chat_id={chat_id_external}, "
                f"pydantic_chat_id={payload.chat_id}, user_id={user_id}, author_id={author_id}"
            )
            if not chat_id_external:
                print(
                    f"[Avito webhook] message event without chat_id (message_id={message_id}, user_id={user_id}), "
                    f"raw_payload_value keys: {list(raw_payload_value.keys()) if isinstance(raw_payload_value, dict) else None}"
                )
                return {
                    "success": False,
                    "message": "Missing chat_id in message payload",
                }

            message_content, message_text = AvitoHandler._extract_message_content(
                payload.content or {}, message_type
            )
            voice_file_bytes: Optional[bytes] = None
            voice_temp_url: Optional[str] = None

            existing_chat = None
            avito_channel = None
            if chat_id_external:
                from database.db import channels, chats, database
                from sqlalchemy import and_, select

                query = (
                    select([chats.c.id, chats.c.channel_id, chats.c.cashbox_id])
                    .select_from(
                        chats.join(channels, chats.c.channel_id == channels.c.id)
                    )
                    .where(
                        and_(
                            channels.c.type == "AVITO",
                            channels.c.is_active.is_(True),
                            chats.c.external_chat_id == str(chat_id_external),
                            chats.c.cashbox_id == cashbox_id,
                        )
                    )
                    .limit(1)
                )
                existing_chat = await database.fetch_one(query)
                if existing_chat:
                    existing_channel_id = existing_chat["channel_id"]
                    avito_channel = await crud.get_channel(existing_channel_id)

            if channel_id:
                avito_channel = await crud.get_channel(channel_id)

            if not avito_channel and user_id is not None:
                from database.db import channel_credentials
                from sqlalchemy import and_, select

                try:
                    user_id_int = (
                        int(user_id) if not isinstance(user_id, int) else user_id
                    )
                except (TypeError, ValueError):
                    user_id_int = None
                if user_id_int is not None:
                    query = (
                        select([channel_credentials.c.channel_id])
                        .select_from(
                            channel_credentials.join(
                                channels,
                                channel_credentials.c.channel_id == channels.c.id,
                            )
                        )
                        .where(
                            and_(
                                channels.c.type == "AVITO",
                                channels.c.is_active.is_(True),
                                channel_credentials.c.avito_user_id == user_id_int,
                                channel_credentials.c.cashbox_id == cashbox_id,
                                channel_credentials.c.is_active.is_(True),
                            )
                        )
                        .limit(1)
                    )
                    creds_result = await database.fetch_one(query)
                else:
                    creds_result = None
                if creds_result:
                    found_channel_id = creds_result["channel_id"]
                    avito_channel = await crud.get_channel(found_channel_id)

            if not avito_channel:
                avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")
                if avito_channel:
                    logger.warning(
                        f"Using fallback channel {avito_channel['id']} for cashbox {cashbox_id} (chat not found, user_id not matched)"
                    )

            if avito_channel:
                if not avito_channel.get("is_active", True):
                    return {"success": True, "message": "Event processed"}

                from database.db import channel_credentials

                creds = await database.fetch_one(
                    channel_credentials.select().where(
                        (channel_credentials.c.channel_id == avito_channel["id"])
                        & (channel_credentials.c.cashbox_id == cashbox_id)
                    )
                )
                if not creds or not creds.get("is_active", True):
                    return {"success": True, "message": "Event processed"}

            if message_type in {"image", "voice", "video", "file", "document"}:
                media_url = AvitoHandler._extract_message_media_file_url(
                    message_content, message_type
                )
                if not media_url:
                    message_content, message_text = (
                        await AvitoHandler._enrich_media_message_from_avito(
                            avito_channel=avito_channel,
                            cashbox_id=cashbox_id,
                            chat_id_external=chat_id_external,
                            message_id=message_id,
                            message_type=message_type,
                            message_content=message_content,
                            message_text=message_text,
                        )
                    )

            if message_type == "voice" and isinstance(message_content, dict):
                voice_id = AvitoHandler._extract_voice_id(message_content)
                if (
                    voice_id
                    and not message_content.get("url")
                    and not message_content.get("voice_url")
                ):
                    try:
                        from api.chats.avito.avito_factory import (
                            create_avito_client,
                            save_token_callback,
                        )

                        if avito_channel:
                            voice_client = await create_avito_client(
                                channel_id=avito_channel["id"],
                                cashbox_id=cashbox_id,
                                on_token_refresh=lambda token_data: save_token_callback(
                                    avito_channel["id"], cashbox_id, token_data
                                ),
                            )
                            if voice_client:
                                voice_url = await voice_client.get_voice_file_url(
                                    voice_id
                                )
                                if voice_url:
                                    message_content["url"] = voice_url
                                    message_content["voice_url"] = voice_url
                                    duration = message_content.get("duration")
                                    voice_temp_url = voice_url
                                    voice_file_bytes = (
                                        await _download_remote_media_bytes(voice_url)
                                    )
                                    if voice_file_bytes:
                                        if duration and isinstance(
                                            duration, (int, float)
                                        ):
                                            message_text = f"[Голосовое сообщение: {int(duration)}с]"
                                        else:
                                            message_text = "[Голосовое сообщение]"
                                    else:
                                        message_content["url"] = voice_url
                                        message_content["voice_url"] = voice_url
                                        if duration and isinstance(
                                            duration, (int, float)
                                        ):
                                            message_text = f"[Voice message: {duration}s - {voice_url}]"
                                        else:
                                            message_text = (
                                                f"[Voice message: {voice_url}]"
                                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to get voice URL for voice_id {voice_id} before message creation: {e}"
                        )

            sender_type = "CLIENT"
            avito_user_id = None
            try:
                from database.db import channel_credentials

                if avito_channel:
                    creds = await database.fetch_one(
                        channel_credentials.select().where(
                            (channel_credentials.c.channel_id == avito_channel["id"])
                            & (channel_credentials.c.cashbox_id == cashbox_id)
                            & (channel_credentials.c.is_active.is_(True))
                        )
                    )
                    if creds and creds.get("avito_user_id"):
                        avito_user_id = creds["avito_user_id"]
                        if author_id is not None and str(author_id) == str(
                            avito_user_id
                        ):
                            sender_type = "OPERATOR"
                        else:
                            sender_type = "CLIENT"
            except Exception as e:
                logger.warning(
                    f"Could not determine message direction, defaulting to CLIENT: {e}"
                )

            user_name = None
            user_phone = None
            user_avatar = None
            context = None
            ad_title = None
            ad_id = None
            ad_url = None
            client_user_id = None
            metadata = {}
            if sender_type == "CLIENT":
                if message_text:
                    if message_type == "system" or (
                        "[Системное сообщение]" in message_text
                        or "системное" in message_text.lower()
                    ):
                        extracted_phone = extract_phone_from_text(message_text)
                        if extracted_phone:
                            user_phone = extracted_phone
                    else:
                        extracted_phone = extract_phone_from_text(message_text)
                        if extracted_phone:
                            user_phone = extracted_phone

                try:
                    if not avito_channel:
                        logger.warning("Avito channel not found, cannot get chat info")
                    else:
                        from api.chats.avito.avito_factory import (
                            create_avito_client,
                            save_token_callback,
                        )

                        client = await create_avito_client(
                            channel_id=avito_channel["id"],
                            cashbox_id=cashbox_id,
                            on_token_refresh=lambda token_data: save_token_callback(
                                avito_channel["id"], cashbox_id, token_data
                            ),
                        )
                        if client:
                            chat_info = await client.get_chat_info(chat_id_external)
                            users = chat_info.get("users", [])
                            if users:
                                candidates = []
                                for user in users:
                                    user_id_in_chat = user.get("user_id") or user.get(
                                        "id"
                                    )
                                    if (
                                        user_id_in_chat
                                        and user_id_in_chat != avito_user_id
                                    ):
                                        candidates.append(user)
                                if candidates:
                                    user = None
                                    if author_id is not None:
                                        for u in candidates:
                                            uid = u.get("user_id") or u.get("id")
                                            if uid is not None and str(uid) == str(
                                                author_id
                                            ):
                                                user = u
                                                break
                                    if user is None:
                                        user = candidates[0]
                                    client_user_id = str(
                                        user.get("user_id") or user.get("id")
                                    )
                                    user_name = (
                                        user.get("name")
                                        or user.get("profile_name")
                                        or (user.get("public_user_profile") or {}).get(
                                            "name"
                                        )
                                        or (user.get("public_user_profile") or {}).get(
                                            "profile_name"
                                        )
                                    )
                                    user_phone_from_api = (
                                        user.get("phone")
                                        or user.get("phone_number")
                                        or (user.get("public_user_profile") or {}).get(
                                            "phone"
                                        )
                                        or (user.get("public_user_profile") or {}).get(
                                            "phone_number"
                                        )
                                    )
                                    if user_phone_from_api:
                                        user_phone = user_phone_from_api
                                    public_profile = user.get("public_user_profile", {})
                                    if public_profile:
                                        avatar_data = public_profile.get("avatar", {})
                                        logger.info(
                                            f"[Avito] avatar_data type={type(avatar_data).__name__}, keys={list(avatar_data.keys()) if isinstance(avatar_data, dict) else 'str'}"
                                        )
                                        if isinstance(avatar_data, dict):
                                            user_avatar = (
                                                avatar_data.get("default")
                                                or (
                                                    avatar_data.get("images") or {}
                                                ).get("256x256")
                                                or (
                                                    avatar_data.get("images") or {}
                                                ).get("128x128")
                                                or (
                                                    list(
                                                        (
                                                            avatar_data.get("images")
                                                            or {}
                                                        ).values()
                                                    )[0]
                                                    if (avatar_data.get("images"))
                                                    else None
                                                )
                                            )
                                        elif isinstance(avatar_data, str):
                                            user_avatar = avatar_data
                                        logger.info(
                                            f"[Avito] user_avatar resolved: {user_avatar}"
                                        )
                                else:
                                    logger.warning(
                                        f"No client users in chat info for {chat_id_external}"
                                    )
                            else:
                                logger.warning(
                                    f"No users in chat info for {chat_id_external}"
                                )

                            if not user_phone:
                                try:
                                    messages = await client.get_messages(
                                        chat_id_external, limit=50
                                    )
                                    for msg in messages:
                                        msg_content = msg.get("content", {})
                                        msg_text = (
                                            msg_content.get("text", "")
                                            if isinstance(msg_content, dict)
                                            else str(msg_content)
                                        )
                                        if msg_text:
                                            extracted_phone = extract_phone_from_text(
                                                msg_text
                                            )
                                            if extracted_phone:
                                                user_phone = extracted_phone
                                                break
                                except Exception as e:
                                    logger.warning(
                                        f"Could not get messages to extract phone: {e}"
                                    )

                            context = chat_info.get("context", {})
                            logger.info(f"[Avito] Chat info users: {users}")
                            logger.info(
                                f"[Avito] Client user: {user_name}, phone: {user_phone}, avatar: {user_avatar}"
                            )
                            ad_title = None
                            ad_id = None
                            ad_url = None
                            if isinstance(context, dict):
                                item = context.get("item") or context.get(
                                    "value", {}
                                ).get("item")
                                if isinstance(item, dict):
                                    ad_title = item.get("title")
                                    ad_id = item.get("id")
                                    ad_url = item.get("url")
                            metadata = {}
                            chat_type = getattr(payload, "chat_type", None)
                            if chat_type == "u2u":
                                metadata = {
                                    "source": "avito",
                                    "username": user_name,
                                    "avito_user_id": client_user_id or author_id,
                                    "chat_type": "u2u",
                                }
                            else:
                                if context:
                                    metadata["context"] = context
                                if ad_title:
                                    metadata["ad_title"] = ad_title
                                if ad_id:
                                    metadata["ad_id"] = ad_id
                                if ad_url:
                                    metadata["ad_url"] = ad_url
                                else:
                                    logger.warning(
                                        "Could not create Avito client to get chat info"
                                    )
                except Exception as e:
                    logger.error(f"Could not get chat info from Avito API: {e}")

            if not user_name:
                if sender_type == "CLIENT":
                    user_name = (
                        f"Avito User {author_id or client_user_id or '?'}"
                        if (author_id or client_user_id)
                        else "Unknown User"
                    )
                else:
                    user_name = None

            if channel_id:
                target_channel = await crud.get_channel(channel_id)
                if target_channel:
                    existing_chat = await crud.get_chat_by_external_id(
                        channel_id=channel_id,
                        external_chat_id=chat_id_external,
                        cashbox_id=cashbox_id,
                    )
                    if existing_chat:
                        chat = existing_chat
                    else:
                        message_text_lower = (
                            message_text.lower() if message_text else ""
                        )
                        subscription_keywords = [
                            "подписк",
                            "мессенджер",
                            "api мессенджера",
                            "subscription",
                            "messenger",
                            "перейдите на подписку",
                        ]
                        is_subscription_message = message_type == "system" or any(
                            keyword in message_text_lower
                            for keyword in subscription_keywords
                        )

                        if is_subscription_message:
                            return {
                                "success": True,
                                "message": "Subscription message ignored, chat not created",
                            }

                        ad_title = None
                        ad_id = None
                        ad_url = None
                        context = None
                        metadata = {}
                        if isinstance(payload, dict):
                            context = payload.get("context", {})
                            if isinstance(context, dict):
                                item = context.get("item", {})
                                if isinstance(item, dict):
                                    ad_title = item.get("title")
                                    ad_id = item.get("id")
                                    ad_url = item.get("url")
                                    if ad_title:
                                        metadata["ad_title"] = ad_title
                                    if ad_id:
                                        metadata["ad_id"] = ad_id
                                    if ad_url:
                                        metadata["ad_url"] = ad_url
                                if context:
                                    metadata["context"] = context

                        chat_name = (
                            ad_title
                            or user_name
                            or f"Avito Chat {chat_id_external[:8]}"
                        )

                        chat = await crud.create_chat(
                            channel_id=channel_id,
                            cashbox_id=cashbox_id,
                            external_chat_id=chat_id_external,
                            external_chat_id_for_contact=(
                                client_user_id if client_user_id else None
                            ),
                            name=chat_name,
                            phone=user_phone,
                            avatar=(
                                await _upload_avito_avatar_to_s3(
                                    avatar_url=user_avatar,
                                    external_contact_id=str(
                                        chat_id_external or "unknown"
                                    ),
                                    cashbox_id=cashbox_id,
                                    channel_id=channel_id,
                                )
                                if user_avatar
                                else None
                            ),
                            metadata=metadata if metadata else None,
                        )
                else:
                    message_text_lower = message_text.lower() if message_text else ""
                    subscription_keywords = [
                        "подписк",
                        "мессенджер",
                        "api мессенджера",
                        "subscription",
                        "messenger",
                        "перейдите на подписку",
                    ]
                    is_subscription_message = message_type == "system" or any(
                        keyword in message_text_lower
                        for keyword in subscription_keywords
                    )

                    if is_subscription_message:
                        return {
                            "success": True,
                            "message": "Subscription message ignored, chat not created",
                        }

                    chat = await AvitoHandler._find_or_create_chat(
                        channel_type="AVITO",
                        external_chat_id=chat_id_external,
                        cashbox_id=cashbox_id,
                        user_id=user_id or 0,
                        webhook_data=payload,
                        user_phone=user_phone,
                        user_name=user_name,
                    )
            else:
                message_text_lower = message_text.lower() if message_text else ""
                subscription_keywords = [
                    "подписк",
                    "мессенджер",
                    "api мессенджера",
                    "subscription",
                    "messenger",
                    "перейдите на подписку",
                ]
                is_subscription_message = message_type == "system" or any(
                    keyword in message_text_lower for keyword in subscription_keywords
                )

                if is_subscription_message:
                    return {
                        "success": True,
                        "message": "Subscription message ignored, chat not created",
                    }

                chat = await AvitoHandler._find_or_create_chat(
                    channel_type="AVITO",
                    external_chat_id=chat_id_external,
                    cashbox_id=cashbox_id,
                    user_id=user_id or 0,
                    webhook_data=payload,
                    user_phone=user_phone,
                    user_name=user_name,
                )

            if not chat:
                logger.error(
                    f"Failed to create or find chat {chat_id_external} for cashbox {cashbox_id}"
                )
                raise Exception(f"Failed to create or find chat {chat_id_external}")

            chat_id = chat["id"]

            if sender_type == "CLIENT":
                update_data = {}
                if user_name and chat.get("name") != user_name:
                    update_data["name"] = user_name
                if user_phone and chat.get("phone") != user_phone:
                    update_data["phone"] = user_phone

                current_metadata = chat.get("metadata") or {}
                if not isinstance(current_metadata, dict):
                    current_metadata = {}

                metadata_updated = False
                new_metadata = current_metadata.copy()

                if ad_title and not new_metadata.get("ad_title"):
                    new_metadata["ad_title"] = ad_title
                    metadata_updated = True
                if ad_id and not new_metadata.get("ad_id"):
                    new_metadata["ad_id"] = ad_id
                    metadata_updated = True
                if ad_url and not new_metadata.get("ad_url"):
                    new_metadata["ad_url"] = ad_url
                    metadata_updated = True
                if context and not new_metadata.get("context"):
                    new_metadata["context"] = context
                    metadata_updated = True

                if not chat.get("metadata") or metadata_updated:
                    update_data["metadata"] = new_metadata if new_metadata else None

                if not chat.get("name"):
                    if ad_title:
                        update_data["name"] = ad_title
                    elif user_name:
                        update_data["name"] = user_name

                if update_data:
                    try:
                        from datetime import datetime

                        from database.db import chat_contacts, chats, database

                        update_data["updated_at"] = datetime.utcnow()
                        await database.execute(
                            chats.update()
                            .where(chats.c.id == chat_id)
                            .values(**update_data)
                        )
                        chat.update(update_data)

                        if chat.get("chat_contact_id"):
                            contact = await database.fetch_one(
                                chat_contacts.select().where(
                                    chat_contacts.c.id == chat["chat_contact_id"]
                                )
                            )

                            if contact:
                                contact_dict = dict(contact) if contact else {}
                                contact_update = {}
                                if user_name and not contact_dict.get("name"):
                                    contact_update["name"] = user_name
                                if user_phone and not contact_dict.get("phone"):
                                    contact_update["phone"] = user_phone
                                if client_user_id and not contact_dict.get(
                                    "external_contact_id"
                                ):
                                    contact_update["external_contact_id"] = str(
                                        client_user_id
                                    )

                                if contact_update:
                                    contact_update["updated_at"] = datetime.utcnow()
                                    await database.execute(
                                        chat_contacts.update()
                                        .where(chat_contacts.c.id == contact_dict["id"])
                                        .values(**contact_update)
                                    )
                    except Exception as e:
                        logger.warning(f"Failed to update chat info: {e}")

            if client_user_id and chat.get("chat_contact_id"):
                try:
                    from datetime import datetime

                    from database.db import chat_contacts, database

                    contact = await database.fetch_one(
                        chat_contacts.select().where(
                            chat_contacts.c.id == chat["chat_contact_id"]
                        )
                    )
                    if contact:
                        contact_dict = dict(contact) if contact else {}
                        if not contact_dict.get(
                            "external_contact_id"
                        ) or contact_dict.get("external_contact_id") != str(
                            client_user_id
                        ):
                            await database.execute(
                                chat_contacts.update()
                                .where(chat_contacts.c.id == contact_dict["id"])
                                .values(
                                    external_contact_id=str(client_user_id),
                                    updated_at=datetime.utcnow(),
                                )
                            )
                except Exception as e:
                    logger.warning(f"Failed to update external_contact_id: {e}")

            from database.db import chat_messages

            existing_message = await database.fetch_one(
                chat_messages.select().where(
                    (chat_messages.c.external_message_id == message_id)
                    & (chat_messages.c.chat_id == chat_id)
                )
            )

            if existing_message:
                return {
                    "success": True,
                    "message": "Message already exists",
                    "chat_id": chat_id,
                    "message_id": existing_message["id"],
                }

            created_at = None
            if hasattr(payload, "created") and payload.created:
                from datetime import datetime

                created_at = datetime.fromtimestamp(payload.created)
            elif hasattr(payload, "published_at") and payload.published_at:
                from datetime import datetime

                try:
                    created_at = datetime.fromisoformat(
                        payload.published_at.replace("Z", "+00:00")
                    )
                except Exception:
                    pass

            try:
                mapped_message_type = AvitoHandler._map_message_type(message_type)
                message = await crud.create_message_and_update_chat(
                    chat_id=chat_id,
                    sender_type=sender_type,
                    content=message_text,
                    message_type=mapped_message_type,
                    external_message_id=message_id,
                    status="DELIVERED",
                    created_at=created_at,
                    source="avito",
                )
            except Exception as save_error:
                logger.error(
                    f"Failed to save message to DB: chat_id={chat_id}, "
                    f"external_message_id={message_id}, error={save_error}"
                )
                raise

            voice_s3_done = False
            if message_type == "voice" and voice_file_bytes:
                try:
                    from database.db import pictures

                    stored_voice_key = await _upload_media_bytes_to_s3(
                        file_bytes=voice_file_bytes,
                        message_id=message["id"],
                        cashbox_id=cashbox_id,
                        channel_id=chat["channel_id"],
                        extension="mp4",
                    )
                    if stored_voice_key:
                        await database.execute(
                            pictures.insert().values(
                                entity="messages",
                                entity_id=message["id"],
                                url=stored_voice_key,
                                is_main=False,
                                is_deleted=False,
                                owner=cashbox_id,
                                cashbox=cashbox_id,
                            )
                        )
                        voice_s3_done = True
                    elif voice_temp_url:
                        await database.execute(
                            pictures.insert().values(
                                entity="messages",
                                entity_id=message["id"],
                                url=voice_temp_url,
                                is_main=False,
                                is_deleted=False,
                                owner=cashbox_id,
                                cashbox=cashbox_id,
                            )
                        )
                        voice_s3_done = True
                except Exception as e:
                    logger.warning(
                        f"Failed to persist Avito voice for message {message['id']}: {e}"
                    )

            file_url = None
            if message_type == "image":
                try:
                    from database.db import pictures

                    file_url = AvitoHandler._extract_message_media_file_url(
                        payload.content or message_content, message_type
                    )

                    if file_url:
                        await database.execute(
                            pictures.insert().values(
                                entity="messages",
                                entity_id=message["id"],
                                url=file_url,
                                is_main=False,
                                is_deleted=False,
                                owner=cashbox_id,
                                cashbox=cashbox_id,
                            )
                        )
                except Exception as e:
                    logger.warning(
                        f"Failed to save image file for message {message['id']}: {e}"
                    )

            elif (
                message_type in {"voice", "video", "file", "document"}
                and isinstance(message_content, dict)
                and not (message_type == "voice" and voice_s3_done)
            ):
                try:
                    from database.db import chats, pictures

                    file_url = AvitoHandler._extract_message_media_file_url(
                        message_content, message_type
                    )

                    if not file_url and message_type == "voice":
                        voice_id = AvitoHandler._extract_voice_id(message_content)
                        if voice_id:
                            try:
                                from api.chats.avito.avito_factory import (
                                    create_avito_client,
                                    save_token_callback,
                                )

                                chat_data = await database.fetch_one(
                                    chats.select().where(chats.c.id == chat_id)
                                )
                                if chat_data:
                                    channel_id = chat_data["channel_id"]
                                    avito_channel = await crud.get_channel(channel_id)
                                    if (
                                        avito_channel
                                        and avito_channel.get("type") == "AVITO"
                                    ):
                                        voice_client = await create_avito_client(
                                            channel_id=channel_id,
                                            cashbox_id=cashbox_id,
                                            on_token_refresh=lambda token_data: save_token_callback(
                                                channel_id,
                                                cashbox_id,
                                                token_data,
                                            ),
                                        )
                                        if voice_client:
                                            file_url = (
                                                await voice_client.get_voice_file_url(
                                                    voice_id
                                                )
                                            )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to get voice URL for voice_id {voice_id}: {e}"
                                )

                    if file_url:
                        chat_data = await database.fetch_one(
                            chats.select().where(chats.c.id == chat_id)
                        )
                        cashbox_id_for_picture = (
                            chat_data["cashbox_id"] if chat_data else cashbox_id
                        )

                        await database.execute(
                            pictures.insert().values(
                                entity="messages",
                                entity_id=message["id"],
                                url=file_url,
                                is_main=False,
                                is_deleted=False,
                                owner=cashbox_id_for_picture,
                                cashbox=cashbox_id_for_picture,
                            )
                        )
                except Exception as e:
                    logger.warning(
                        f"Failed to save {message_type} file for message {message['id']}: {e}"
                    )

            try:
                await chat_producer.send_message(
                    chat_id,
                    {
                        "message_id": message["id"],
                        "chat_id": chat_id,
                        "channel_type": "AVITO",
                        "external_message_id": message_id,
                        "sender_type": "CLIENT",
                        "content": message_text,
                        "message_type": mapped_message_type,
                        "created_at": datetime.utcnow().isoformat(),
                        "user_id": user_id,
                    },
                )
            except Exception as e:
                logger.error(f"Failed to send message to RabbitMQ: {e}")

            try:
                from api.chats.websocket import cashbox_manager, chat_manager

                ws_message = {
                    "type": "chat_message",
                    "event": "new_message",
                    "chat_id": chat_id,
                    "message_id": message["id"],
                    "sender_type": sender_type,
                    "content": message_text,
                    "message_type": mapped_message_type,
                    "timestamp": datetime.utcnow().isoformat(),
                }
                media_url = await crud.get_message_file_url(
                    message["id"], mapped_message_type
                )
                if mapped_message_type == MessageType.IMAGE and media_url:
                    ws_message["image_url"] = media_url
                elif mapped_message_type in crud.MEDIA_FILE_TYPES and media_url:
                    ws_message["file_url"] = media_url

                chat_payload = {
                    "type": "message",
                    "chat_id": chat_id,
                    "message_id": message["id"],
                    "sender_type": sender_type,
                    "content": message_text,
                    "message_type": mapped_message_type,
                    "status": "DELIVERED",
                    "timestamp": datetime.utcnow().isoformat(),
                }
                if ws_message.get("image_url"):
                    chat_payload["image_url"] = ws_message["image_url"]
                if ws_message.get("file_url"):
                    chat_payload["file_url"] = ws_message["file_url"]

                await chat_manager.broadcast_to_chat(chat_id, chat_payload)

                await cashbox_manager.broadcast_to_cashbox(cashbox_id, ws_message)
            except Exception as e:
                logger.warning(f"Failed to send WebSocket event: {e}")

            return {
                "success": True,
                "message": "Message processed successfully",
                "chat_id": chat_id,
                "message_id": message["id"],
            }

        except Exception as e:
            logger.error(
                f"Error processing Avito webhook: {e}. "
                f"chat_id_external={chat_id_external if 'chat_id_external' in locals() else 'unknown'}, "
                f"message_id={message_id if 'message_id' in locals() else 'unknown'}, "
                f"cashbox_id={cashbox_id}"
            )
            return {
                "success": False,
                "message": f"Failed to process message: {str(e)}",
                "error": str(e),
            }

    @staticmethod
    async def _find_or_create_chat(
        channel_type: str,
        external_chat_id: str,
        cashbox_id: int,
        user_id: int,
        webhook_data: Dict[str, Any],
        user_phone: Optional[str] = None,
        user_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            external_chat_id = str(external_chat_id).strip() if external_chat_id else ""
            if not external_chat_id:
                logger.error("external_chat_id is empty after cast to str")
                return None

            if channel_type == "AVITO":
                from database.db import channels, chats, database
                from sqlalchemy import and_, select

                # 1. Проверяем, существует ли уже чат
                existing_chat_query = (
                    select([chats.c.id, chats.c.channel_id, chats.c.cashbox_id])
                    .select_from(
                        chats.join(channels, chats.c.channel_id == channels.c.id)
                    )
                    .where(
                        and_(
                            channels.c.type == "AVITO",
                            channels.c.is_active.is_(True),
                            chats.c.external_chat_id == external_chat_id,  # уже str
                            chats.c.cashbox_id == cashbox_id,
                        )
                    )
                    .limit(1)
                )
                existing_chat_result = await database.fetch_one(existing_chat_query)
                if existing_chat_result:
                    return await crud.get_chat(existing_chat_result["id"])

                # 2. Определяем канал
                channel = None
                if user_id and user_id > 0:
                    from database.db import channel_credentials

                    query = (
                        select([channel_credentials.c.channel_id])
                        .select_from(
                            channel_credentials.join(
                                channels,
                                channel_credentials.c.channel_id == channels.c.id,
                            )
                        )
                        .where(
                            and_(
                                channels.c.type == "AVITO",
                                channels.c.is_active.is_(True),
                                channel_credentials.c.avito_user_id == user_id,
                                channel_credentials.c.cashbox_id == cashbox_id,
                                channel_credentials.c.is_active.is_(True),
                            )
                        )
                        .limit(1)
                    )
                    creds_result = await database.fetch_one(query)
                    if creds_result:
                        channel = await crud.get_channel(creds_result["channel_id"])

                if not channel:
                    channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")
                    if channel:
                        logger.warning(
                            f"Using fallback channel {channel['id']} for cashbox {cashbox_id} "
                            f"(user_id {user_id} not matched)"
                        )

                if not channel:
                    logger.warning(
                        f"Avito channel not found for cashbox {cashbox_id}. "
                        f"Channel should be created via /connect endpoint first."
                    )
                    return None

            else:
                channel = await crud.get_channel_by_type(channel_type)
                if not channel:
                    logger.warning(
                        f"Channel {channel_type} not found, creating new one"
                    )
                    channel = await crud.create_channel(
                        name=channel_type,
                        type=channel_type,
                        description=f"{channel_type} integration channel",
                    )

            existing_chat = await crud.get_chat_by_external_id(
                channel_id=channel["id"],
                external_chat_id=external_chat_id,
                cashbox_id=cashbox_id,
            )
            if existing_chat:
                return existing_chat

            ad_title: Optional[str] = None
            ad_id: Optional[int] = None
            ad_url: Optional[str] = None
            metadata: Dict[str, Any] = {}
            client_user_id = None
            final_user_name = user_name
            final_user_phone = user_phone

            if channel_type == "AVITO":
                try:
                    from api.chats.avito.avito_factory import (
                        create_avito_client,
                        save_token_callback,
                    )

                    client = await create_avito_client(
                        channel_id=channel["id"],
                        cashbox_id=cashbox_id,
                        on_token_refresh=lambda td: save_token_callback(
                            channel["id"], cashbox_id, td
                        ),
                    )
                    if client:
                        chat_info = await client.get_chat_info(external_chat_id)
                        chat_type = None
                        if isinstance(webhook_data, dict):
                            chat_type = webhook_data.get("chat_type")
                        if not chat_type and chat_info:
                            pass
                        if chat_info:
                            from database.db import channel_credentials

                            creds = await database.fetch_one(
                                channel_credentials.select().where(
                                    (channel_credentials.c.channel_id == channel["id"])
                                    & (channel_credentials.c.cashbox_id == cashbox_id)
                                    & (channel_credentials.c.is_active.is_(True))
                                )
                            )
                            avito_user_id = (
                                creds.get("avito_user_id") if creds else None
                            )

                            users = chat_info.get("users", [])
                            for user in users:
                                uid = user.get("user_id") or user.get("id")
                                if uid and str(uid) != str(avito_user_id):
                                    client_user_id = str(uid)
                                    if not final_user_name:
                                        final_user_name = user.get("name") or user.get(
                                            "profile_name"
                                        )
                                    if not final_user_phone:
                                        phone_from_api = (
                                            user.get("phone")
                                            or user.get("phone_number")
                                            or user.get("public_user_profile", {}).get(
                                                "phone"
                                            )
                                            or user.get("public_user_profile", {}).get(
                                                "phone_number"
                                            )
                                        )
                                        if phone_from_api:
                                            final_user_phone = phone_from_api
                                    break

                            if chat_type == "u2u":
                                metadata = {
                                    "source": "avito",
                                    "username": final_user_name,
                                    "avito_user_id": client_user_id,
                                    "chat_type": "u2u",
                                }
                            else:
                                context = chat_info.get("context", {})
                                if isinstance(context, dict):
                                    item = context.get("item", {})
                                    if isinstance(item, dict):
                                        ad_title = item.get("title")
                                        ad_id = item.get("id")
                                        ad_url = item.get("url")

                                        if ad_title:
                                            metadata["ad_title"] = ad_title
                                        if ad_id is not None:
                                            metadata["ad_id"] = ad_id
                                        if ad_url:
                                            metadata["ad_url"] = ad_url

                                    if context:
                                        metadata["context"] = context

                            logger.info(
                                f"[Avito] get_chat_info → metadata: ad_title={ad_title}, ad_id={ad_id}"
                            )

                except Exception as e:
                    logger.warning(f"Could not get chat info from Avito API: {e}")

            if not metadata or not metadata.get("context"):
                try:
                    if hasattr(webhook_data, "__dict__"):
                        webhook_dict = webhook_data.__dict__
                    elif isinstance(webhook_data, dict):
                        webhook_dict = webhook_data
                    else:
                        webhook_dict = {}

                    item_id = None
                    search_paths = [
                        ["item_id"],
                        ["itemId"],
                        ["item", "id"],
                        ["value", "item_id"],
                        ["value", "itemId"],
                        ["value", "item", "id"],
                    ]

                    for path in search_paths:
                        val = webhook_dict
                        for key in path:
                            if isinstance(val, dict):
                                val = val.get(key)
                            else:
                                val = None
                                break
                        if val is not None:
                            item_id = val
                            break

                    if item_id is not None and str(item_id) != "0":
                        metadata = {
                            "context": {"type": "item", "value": {"id": item_id}},
                            "ad_id": item_id,
                        }
                        logger.info(
                            f"[Avito] webhook fallback → real item_id={item_id}"
                        )
                    else:
                        metadata = {"context": {"type": "unknown"}}
                        logger.info(
                            f"[Avito] webhook fallback → unknown context for chat {external_chat_id}"
                        )

                except Exception as e:
                    logger.warning(f"Failed to parse item_id from webhook: {e}")
                    metadata = {"context": {"type": "unknown"}}

            chat_name = (
                ad_title or final_user_name or f"Avito Chat {external_chat_id[:8]}"
            )

            new_chat = await crud.create_chat(
                channel_id=channel["id"],
                cashbox_id=cashbox_id,
                external_chat_id=external_chat_id,
                external_chat_id_for_contact=(
                    str(client_user_id) if client_user_id else None
                ),
                name=chat_name,
                phone=final_user_phone,
                metadata=metadata if metadata else None,
            )

            logger.info(
                f"[Avito] _find_or_create_chat SUCCESS → chat_id={new_chat['id']}, "
                f"metadata={new_chat.get('metadata')}"
            )
            return new_chat

        except Exception as e:
            logger.error(f"Failed to find/create chat in _find_or_create_chat: {e}")
            return None

    @staticmethod
    def _extract_message_content(
        content: Dict[str, Any], message_type: str
    ) -> tuple[Dict[str, Any], str]:
        if not content:
            if message_type == "voice":
                return {}, "[Voice message]"
            if message_type == "video":
                return {}, "[Видео: просмотр невозможен через API Avito]"
            if message_type in {"file", "document"}:
                return {}, "[Document]"
            return {}, ""

        message_content = content.copy() if isinstance(content, dict) else {}
        message_text = ""

        if message_type == "text":
            message_text = (
                content.get("text", "") if isinstance(content, dict) else str(content)
            )

        elif message_type == "image":
            if isinstance(content, dict) and "image" in content:
                message_content = content["image"]
                sizes = (
                    message_content.get("sizes", {})
                    if isinstance(message_content, dict)
                    else {}
                )
                if isinstance(sizes, dict):
                    image_url = (
                        sizes.get("1280x960")
                        or sizes.get("640x480")
                        or (list(sizes.values())[0] if sizes else None)
                    )
                    message_text = ""
                else:
                    message_text = ""
            else:
                message_text = ""

        elif message_type == "voice":
            if isinstance(content, dict) and "voice" in content:
                message_content = content["voice"]
                duration = content["voice"].get("duration")
                voice_url = AvitoHandler._extract_message_media_file_url(
                    content["voice"], "voice"
                )
                voice_id = AvitoHandler._extract_voice_id(content["voice"])
                if voice_id and not voice_url:
                    message_content["voice_id"] = voice_id

                if voice_url:
                    if duration and isinstance(duration, (int, float)):
                        message_text = f"[Voice message: {duration}s - {voice_url}]"
                    else:
                        message_text = f"[Voice message: {voice_url}]"
                elif voice_id:
                    if duration and isinstance(duration, (int, float)):
                        message_text = (
                            f"[Voice message: {duration}s - voice_id: {voice_id}]"
                        )
                    else:
                        message_text = f"[Voice message: voice_id: {voice_id}]"
                else:
                    if duration and isinstance(duration, (int, float)):
                        message_text = f"[Voice message: {duration}s]"
                    else:
                        message_text = "[Voice message]"
            else:
                message_text = "[Voice message]"

        elif message_type == "video":
            if isinstance(content, dict) and "video" in content:
                message_content = content["video"]
                video_url = AvitoHandler._extract_message_media_file_url(
                    content["video"], "video"
                )
                message_text = (
                    f"[Video: {video_url}]"
                    if video_url
                    else "[Видео: просмотр невозможен через API Avito]"
                )
            else:
                message_text = "[Видео: просмотр невозможен через API Avito]"

        elif message_type == "file" or message_type == "document":
            if isinstance(content, dict) and "file" in content:
                message_content = content["file"]
                file_url = AvitoHandler._extract_message_media_file_url(
                    content["file"], "file"
                )
                file_name = content["file"].get("name", "document")
                message_text = (
                    f"[File: {file_name} - {file_url}]"
                    if file_url
                    else f"[File: {file_name}]"
                )
            else:
                message_text = "[Document]"

        elif message_type == "link":
            if isinstance(content, dict) and "link" in content:
                message_content = content["link"]
                message_text = content["link"].get("text") or content["link"].get(
                    "url", "[Link]"
                )
            else:
                message_text = "[Link]"

        elif message_type == "location":
            if isinstance(content, dict) and "location" in content:
                message_content = content["location"]
                message_text = f"[Location: {content['location'].get('latitude')}, {content['location'].get('longitude')}]"
            else:
                message_text = "[Location]"

        elif message_type == "item":
            if isinstance(content, dict) and "item" in content:
                message_content = content["item"]
                message_text = f"[Item: {content['item'].get('title', 'No title')}]"
            else:
                message_text = "[Avito item]"

        elif message_type == "system":
            message_text = (
                content.get("text", "[System message]")
                if isinstance(content, dict)
                else "[System message]"
            )

        else:
            message_text = (
                str(content)
                if not isinstance(content, dict)
                else "[Unknown message type]"
            )

        return message_content, message_text

    @staticmethod
    def _map_message_type(avito_type: str) -> str:
        mapping = {
            "text": "TEXT",
            "image": "IMAGE",
            "voice": "VOICE",
            "video": "VIDEO",
            "file": "DOCUMENT",
            "item": "DOCUMENT",
            "location": "TEXT",
            "link": "TEXT",
            "appCall": "TEXT",
            "system": "SYSTEM",
            "deleted": "TEXT",
        }
        return mapping.get(avito_type, "TEXT")

    @staticmethod
    async def handle_webhook_event(
        webhook: AvitoWebhook,
        cashbox_id: int,
        channel_id: Optional[int] = None,
        raw_payload_value: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        event_type = (webhook.payload.type or "").strip().lower()

        if event_type == "message":
            result = await AvitoHandler.handle_message_event(
                webhook, cashbox_id, channel_id, raw_payload_value=raw_payload_value
            )
            return result

        elif event_type == "status":
            return {
                "success": True,
                "message": "Status event received (not implemented)",
            }

        elif event_type == "typing":
            return {"success": True, "message": "Typing event received"}

        else:
            logger.warning(f"Unknown event type: {event_type}")
            return {"success": False, "message": f"Unknown event type: {event_type}"}

    @staticmethod
    async def sync_messages_from_avito(
        client, chat_id: int, cashbox_id: int, external_chat_id: str
    ) -> Dict[str, Any]:
        try:
            avito_messages = await client.sync_messages(external_chat_id)

            if not avito_messages:
                return {
                    "success": True,
                    "synced_count": 0,
                    "new_messages": 0,
                    "updated_messages": 0,
                    "errors": [],
                }

            synced_count = len(avito_messages)
            new_messages = 0
            updated_messages = 0
            errors = []
            from database.db import chats

            chat_row = await database.fetch_one(
                chats.select().where(chats.c.id == chat_id)
            )
            channel_id = chat_row["channel_id"] if chat_row else None

            for msg_idx, avito_msg in enumerate(avito_messages):
                try:
                    message_id = avito_msg.get("id")
                    direction = avito_msg.get("direction", "in")

                    existing_message = await crud.get_message_by_external_id(
                        chat_id=chat_id, external_message_id=message_id
                    )

                    if existing_message:
                        updated_messages += 1
                        continue

                    content = avito_msg.get("content", {})
                    message_type_str = avito_msg.get("type", "text")

                    if message_type_str == "deleted":
                        continue

                    _, message_text = AvitoHandler._extract_message_content(
                        content, message_type_str
                    )
                    if not message_text:
                        if isinstance(content, str) and content:
                            message_text = content
                        else:
                            message_text = f"[{message_type_str}]"

                    if message_text:
                        message_text_lower = message_text.lower().strip()
                        if (
                            message_text_lower == "[deleted]"
                            or message_text_lower == "сообщение удалено"
                            or "[deleted]" in message_text_lower
                        ):
                            continue

                    sender_type = "CLIENT" if direction == "in" else "OPERATOR"

                    created_timestamp = avito_msg.get("created")
                    created_at = None
                    if created_timestamp:
                        from datetime import datetime

                        created_at = datetime.fromtimestamp(created_timestamp)

                    is_read = (
                        avito_msg.get("is_read", False)
                        or avito_msg.get("read") is not None
                    )
                    status = "READ" if is_read else "DELIVERED"

                    message = await crud.create_message_and_update_chat(
                        chat_id=chat_id,
                        sender_type=sender_type,
                        content=message_text,
                        message_type=AvitoHandler._map_message_type(message_type_str),
                        external_message_id=message_id,
                        status=status,
                        source="avito",
                        created_at=created_at,
                    )

                    db_message_id = (
                        message.get("id") if isinstance(message, dict) else message.id
                    )

                    if message_type_str in [
                        "image",
                        "voice",
                        "video",
                        "file",
                        "document",
                    ]:
                        if isinstance(content, dict):
                            try:
                                from database.db import pictures

                                file_url = None
                                if message_type_str == "voice" and channel_id:
                                    voice_id = AvitoHandler._extract_voice_id(content)
                                    if voice_id:
                                        voice_temp_url = (
                                            await client.get_voice_file_url(voice_id)
                                        )
                                        voice_file_bytes = (
                                            await _download_remote_media_bytes(
                                                voice_temp_url
                                            )
                                            if voice_temp_url
                                            else None
                                        )
                                        if voice_file_bytes:
                                            file_url = await _upload_media_bytes_to_s3(
                                                file_bytes=voice_file_bytes,
                                                message_id=db_message_id,
                                                cashbox_id=cashbox_id,
                                                channel_id=channel_id,
                                                extension="mp4",
                                            )
                                        if not file_url:
                                            file_url = (
                                                voice_temp_url
                                                or AvitoHandler._extract_message_media_file_url(
                                                    content, message_type_str
                                                )
                                            )
                                    else:
                                        file_url = AvitoHandler._extract_message_media_file_url(
                                            content, message_type_str
                                        )
                                else:
                                    file_url = (
                                        AvitoHandler._extract_message_media_file_url(
                                            content, message_type_str
                                        )
                                    )

                                if file_url:
                                    await database.execute(
                                        pictures.insert().values(
                                            entity="messages",
                                            entity_id=db_message_id,
                                            url=file_url,
                                            is_main=False,
                                            is_deleted=False,
                                            owner=cashbox_id,
                                            cashbox=cashbox_id,
                                        )
                                    )
                            except Exception as pic_error:
                                logger.warning(
                                    f"Failed to save {message_type_str} for message {message_id}: {pic_error}"
                                )

                    new_messages += 1

                except Exception as msg_error:
                    logger.error(
                        f"Error syncing message {avito_msg.get('id')}: {msg_error}"
                    )
                    errors.append(
                        {"message_id": avito_msg.get("id"), "error": str(msg_error)}
                    )

            return {
                "success": True,
                "synced_count": synced_count,
                "new_messages": new_messages,
                "updated_messages": updated_messages,
                "errors": errors,
            }

        except Exception as e:
            logger.error(f"Failed to sync messages for chat {chat_id}: {e}")
            return {
                "success": False,
                "synced_count": 0,
                "new_messages": 0,
                "updated_messages": 0,
                "errors": [{"error": str(e)}],
            }

    @staticmethod
    async def handle_status_event(
        webhook: AvitoWebhook, cashbox_id: int
    ) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value

            message_id = payload.get("id")
            chat_id_external = payload.chat_id or ""
            status = payload.get("status")

            chat = await crud.get_chat_by_external_id(
                channel_id=None,
                external_chat_id=chat_id_external,
                cashbox_id=cashbox_id,
            )

            if not chat:
                logger.warning(f"Chat {chat_id_external} not found for status event")
                return {
                    "success": False,
                    "message": f"Chat {chat_id_external} not found",
                }

            message = await crud.get_message_by_external_id(
                chat_id=chat["id"], external_message_id=message_id
            )

            if not message:
                logger.warning(f"Message {message_id} not found in chat {chat['id']}")
                return {"success": False, "message": f"Message {message_id} not found"}

            if status == "read":
                new_status = "READ"
            elif status == "deleted":
                new_status = "DELETED"
            else:
                new_status = status.upper() if status else "UNKNOWN"

            await crud.update_message(message["id"], status=new_status)

            return {
                "success": True,
                "message": f"Status updated to {new_status}",
                "message_id": message["id"],
            }

        except Exception as e:
            logger.error(f"Error processing status event: {e}")
            return {
                "success": False,
                "message": f"Error processing status event: {str(e)}",
            }

    @staticmethod
    async def handle_typing_event(
        webhook: AvitoWebhook, cashbox_id: int
    ) -> Dict[str, Any]:
        try:
            payload = webhook.payload.value

            chat_id_external = payload.chat_id or ""
            is_typing = payload.get("isTyping", False)
            user_id = payload.get("authorId")

            chat = await crud.get_chat_by_external_id(
                channel_id=None,
                external_chat_id=chat_id_external,
                cashbox_id=cashbox_id,
            )

            if not chat:
                logger.warning(f"Chat {chat_id_external} not found for typing event")
                return {
                    "success": False,
                    "message": f"Chat {chat_id_external} not found",
                }

            return {
                "success": True,
                "message": f"Typing event processed for chat {chat['id']}",
                "chat_id": chat["id"],
                "user_typing": is_typing,
            }

        except Exception as e:
            logger.error(f"Error processing typing event: {e}")
            return {
                "success": False,
                "message": f"Error processing typing event: {str(e)}",
            }


async def _get_avito_bot_user_id(channel_id: int, cashbox_id: int) -> Optional[int]:
    from database.db import channel_credentials

    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel_id)
            & (channel_credentials.c.cashbox_id == cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    return creds["avito_user_id"] if creds else None
