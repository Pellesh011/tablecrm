import asyncio
import logging
import re
import secrets
from datetime import datetime
from typing import Optional

from api.chats import crud
from api.chats.auth import get_current_user_for_avito as get_current_user
from api.chats.avito.avito_client import AvitoClient
from api.chats.avito.avito_factory import (
    _encrypt_credential,
    create_avito_client,
    save_token_callback,
)
from api.chats.avito.avito_handler import AvitoHandler
from api.chats.avito.schemas import (
    AvitoConnectResponse,
    AvitoCredentialsCreate,
    AvitoHistoryLoadResponse,
    AvitoOAuthCallbackResponse,
)
from common.utils.url_helper import get_app_url_for_environment
from database.db import channel_credentials, channels, chat_messages, chats, database
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, desc, select, update

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chats/avito", tags=["chats-avito"])

AVITO_OAUTH_CLIENT_ID = "KIaI_T0JMKK-9FOly0HE"
AVITO_OAUTH_CLIENT_SECRET = "H4swEkou67ucS5tqJFPWJ24f5MFdzTu-zXdJ51Qp"
AVITO_OAUTH_SCOPE = "messenger:read,messenger:write,items:info,job:cv,job:applications,job:vacancy,short_term_rent:read,stats:read,user:read,user_operations:read"


def _get_avito_app_url() -> str:
    url = (get_app_url_for_environment() or "").rstrip("/")
    if not url:
        return "https://app.tablecrm.com"
    if not url.startswith("http"):
        url = f"https://{url}"
    return url


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


@router.post("/connect", response_model=AvitoConnectResponse)
async def connect_avito_channel(
    credentials: AvitoCredentialsCreate, user=Depends(get_current_user)
):
    try:
        cashbox_id = user.cashbox_id
        grant_type = credentials.grant_type or "client_credentials"

        if grant_type == "client_credentials":
            if not credentials.api_key or not credentials.api_secret:
                raise HTTPException(
                    status_code=422,
                    detail="api_key and api_secret are required for personal authorization (client_credentials)",
                )

        encrypted_api_key = (
            _encrypt_credential(credentials.api_key) if credentials.api_key else None
        )
        encrypted_api_secret = (
            _encrypt_credential(credentials.api_secret)
            if credentials.api_secret
            else None
        )

        if grant_type == "client_credentials" and encrypted_api_key:

            avito_channel = await crud.get_channel_by_cashbox_and_api_key(
                cashbox_id, encrypted_api_key, "AVITO"
            )
            if not avito_channel:

                avito_channel = await crud.get_channel_by_api_key(
                    encrypted_api_key, "AVITO"
                )
        else:
            avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")

        if not avito_channel:
            channel_name = credentials.channel_name or f"Avito - Cashbox {cashbox_id}"
            from api.chats.avito.avito_constants import AVITO_SVG_ICON

            channel_id = await database.execute(
                channels.insert().values(
                    name=channel_name,
                    type="AVITO",
                    cashbox_id=cashbox_id,
                    svg_icon=AVITO_SVG_ICON,
                    description=f"Avito White API Integration for Cashbox {cashbox_id}",
                    is_active=True,
                )
            )
            avito_channel = await crud.get_channel(channel_id)

        channel_id = avito_channel["id"]

        redirect_uri = (
            credentials.redirect_uri
            or f"{_get_avito_app_url()}/api/v1/hook/chat/123456"
        )

        existing = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == channel_id)
                & (channel_credentials.c.cashbox_id == cashbox_id)
                & (channel_credentials.c.is_active.is_(True))
            )
        )

        update_values = {
            "redirect_uri": redirect_uri,
            "is_active": True,
            "updated_at": datetime.utcnow(),
        }

        if grant_type == "client_credentials":
            update_values["api_key"] = encrypted_api_key
            update_values["api_secret"] = encrypted_api_secret
        else:
            update_values["api_key"] = _encrypt_credential(AVITO_OAUTH_CLIENT_ID)
            update_values["api_secret"] = _encrypt_credential(AVITO_OAUTH_CLIENT_SECRET)

        if grant_type == "client_credentials":
            logger.info(
                f"Personal authorization for cashbox={cashbox_id}, getting access_token"
            )

            try:
                from api.chats.avito.avito_client import AvitoClient

                temp_client = AvitoClient(
                    api_key=credentials.api_key, api_secret=credentials.api_secret
                )
                token_data = await temp_client.get_access_token()

                access_token = token_data.get("access_token")
                expires_at = token_data.get("expires_at")

                if not access_token:
                    raise HTTPException(
                        status_code=400,
                        detail="Не удалось получить access_token от Avito",
                    )

                temp_client.access_token = access_token
                avito_user_id = await temp_client._get_user_id()

                update_values["access_token"] = _encrypt_credential(access_token)
                update_values["token_expires_at"] = (
                    datetime.fromisoformat(expires_at) if expires_at else None
                )
                update_values["avito_user_id"] = avito_user_id
                update_values["refresh_token"] = None

                logger.info(
                    f"Successfully obtained access_token for cashbox={cashbox_id}, avito_user_id={avito_user_id}"
                )

            except Exception as e:
                logger.error(f"Failed to get access_token: {e}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Не удалось получить токен от Avito: {str(e)}",
                )

        if existing:
            await database.execute(
                channel_credentials.update()
                .where(channel_credentials.c.id == existing["id"])
                .values(**update_values)
            )
        else:
            insert_values = {
                "channel_id": channel_id,
                "cashbox_id": cashbox_id,
                **update_values,
                "created_at": datetime.utcnow(),
            }
            await database.execute(channel_credentials.insert().values(**insert_values))

        webhook_registered = False
        webhook_error_message = None
        webhook_url = None

        if grant_type == "client_credentials":
            try:
                webhook_url = f"{_get_avito_app_url()}/api/v1/avito/hook/{cashbox_id}"

                async def save_token_callback(channel_id, cashbox_id, token_data):
                    encrypted_access = _encrypt_credential(
                        token_data.get("access_token")
                    )
                    expires_at = token_data.get("expires_at")
                    token_expires_at = (
                        datetime.fromisoformat(expires_at) if expires_at else None
                    )

                    await database.execute(
                        channel_credentials.update()
                        .where(
                            (channel_credentials.c.channel_id == channel_id)
                            & (channel_credentials.c.cashbox_id == cashbox_id)
                        )
                        .values(
                            access_token=encrypted_access,
                            token_expires_at=token_expires_at,
                            updated_at=datetime.utcnow(),
                        )
                    )

                client = await create_avito_client(
                    channel_id=channel_id,
                    cashbox_id=cashbox_id,
                    on_token_refresh=lambda token_data: save_token_callback(
                        channel_id, cashbox_id, token_data
                    ),
                )

                if client:
                    try:
                        result = await client.register_webhook(webhook_url)
                        webhook_registered = True
                        logger.info(
                            f"Webhook registered successfully for channel={channel_id}"
                        )
                    except Exception as webhook_error:
                        webhook_error_message = str(webhook_error)
                        logger.warning(
                            f"Failed to register webhook: {webhook_error_message}"
                        )
                else:
                    webhook_error_message = "Could not create Avito client"

            except Exception as e:
                webhook_error_message = str(e)
                logger.warning(f"Error during webhook registration: {e}")

        if grant_type == "client_credentials":
            response = {
                "success": True,
                "message": "Канал успешно подключен через персональную авторизацию",
                "channel_id": channel_id,
                "cashbox_id": cashbox_id,
                "authorization_url": None,
                "webhook_registered": webhook_registered,
                "webhook_url": webhook_url if webhook_registered else None,
                "webhook_error": webhook_error_message,
            }
        else:
            oauth_client_id = AVITO_OAUTH_CLIENT_ID
            oauth_scope = AVITO_OAUTH_SCOPE

            state = secrets.token_urlsafe(32)
            state_data = f"{cashbox_id}_{state}"

            auth_url = f"https://avito.ru/oauth?response_type=code&client_id={oauth_client_id}&scope={oauth_scope}&state={state_data}"

            logger.info(
                f"OAuth authorization URL generated for cashbox={cashbox_id}, client_id={oauth_client_id}"
            )

            response = {
                "success": True,
                "message": "Credentials saved. Please authorize via OAuth to complete connection.",
                "channel_id": channel_id,
                "cashbox_id": cashbox_id,
                "authorization_url": auth_url,
            }

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error connecting Avito channel: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка подключения: {str(e)}")


async def process_single_chat(
    avito_chat: dict,
    client: AvitoClient,
    channel_id: int,
    cashbox_id: int,
    from_date: int,
    use_date_filter: bool,
    semaphore: asyncio.Semaphore,
) -> dict:
    async with semaphore:
        result = {
            "chats_processed": 0,
            "chats_created": 0,
            "chats_updated": 0,
            "messages_loaded": 0,
            "messages_created": 0,
            "messages_updated": 0,
            "errors": [],
        }

        try:
            external_chat_id = avito_chat.get("id")
            if not external_chat_id:
                return result

            result["chats_processed"] = 1

            users = avito_chat.get("users", [])
            user_name = None
            user_phone = None
            user_avatar = None
            client_user_id = None

            from database.db import channel_credentials as cc

            creds = await database.fetch_one(
                cc.select().where(
                    (cc.c.channel_id == channel_id)
                    & (cc.c.cashbox_id == cashbox_id)
                    & (cc.c.is_active.is_(True))
                )
            )
            avito_user_id = creds.get("avito_user_id") if creds else None

            if users and avito_user_id:
                for user in users:
                    user_id_in_chat = user.get("user_id") or user.get("id")
                    if user_id_in_chat and user_id_in_chat != avito_user_id:
                        client_user_id = user_id_in_chat
                        user_name = user.get("name") or user.get("profile_name")
                        user_phone = (
                            user.get("phone")
                            or user.get("phone_number")
                            or user.get("public_user_profile", {}).get("phone")
                            or user.get("public_user_profile", {}).get("phone_number")
                        )
                        public_profile = user.get("public_user_profile", {})
                        if public_profile:
                            avatar_data = public_profile.get("avatar", {})
                            if isinstance(avatar_data, dict):
                                user_avatar = (
                                    avatar_data.get("default")
                                    or avatar_data.get("images", {}).get("256x256")
                                    or avatar_data.get("images", {}).get("128x128")
                                    or (
                                        list(avatar_data.get("images", {}).values())[0]
                                        if avatar_data.get("images")
                                        else None
                                    )
                                )
                            elif isinstance(avatar_data, str):
                                user_avatar = avatar_data
                        if user_name or user_phone:
                            break

            if not user_phone:
                last_message = avito_chat.get("last_message", {})
                if last_message:
                    message_content = last_message.get("content", {})
                    message_text = None
                    if isinstance(message_content, dict):
                        message_text = message_content.get("text", "")
                    elif isinstance(message_content, str):
                        message_text = message_content

                    if message_text and (
                        "[Системное сообщение]" in message_text
                        or "системное" in message_text.lower()
                    ):
                        user_phone = extract_phone_from_text(message_text)

            from sqlalchemy import select

            existing_chat_query = select(
                [
                    chats.c.id,
                    chats.c.channel_id,
                    chats.c.external_chat_id,
                    chats.c.cashbox_id,
                    chats.c.status,
                    chats.c.chat_contact_id,
                    chats.c.assigned_operator_id,
                    chats.c.first_message_time,
                    chats.c.last_message_time,
                    chats.c.created_at,
                    chats.c.updated_at,
                ]
            ).where(
                (chats.c.channel_id == channel_id)
                & (chats.c.external_chat_id == external_chat_id)
                & (chats.c.cashbox_id == cashbox_id)
            )
            existing_chat = await database.fetch_one(existing_chat_query)

            chat_id = None
            if not existing_chat:
                messages_since = from_date if use_date_filter else None
                try:
                    test_messages = await client.sync_messages(
                        chat_id=external_chat_id, since_timestamp=messages_since
                    )
                    avito_messages = test_messages
                except Exception as test_error:
                    error_str = str(test_error)
                    if (
                        "402" in error_str
                        or "подписку" in error_str.lower()
                        or "subscription" in error_str.lower()
                    ):
                        return result
                    else:
                        avito_messages = []
                else:
                    metadata = {}
                    context = avito_chat.get("context", {})
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
                        user_name
                        or (metadata.get("ad_title") if metadata else None)
                        or f"Avito Chat {external_chat_id[:8]}"
                    )
                    new_chat = await crud.create_chat(
                        channel_id=channel_id,
                        cashbox_id=cashbox_id,
                        external_chat_id=external_chat_id,
                        external_chat_id_for_contact=(
                            str(client_user_id) if client_user_id else None
                        ),
                        name=chat_name,
                        phone=user_phone,
                        avatar=user_avatar,
                        metadata=metadata if metadata else None,
                    )

                    if not new_chat or not new_chat.get("id"):
                        logger.error(
                            f"Failed to create chat with external_id {external_chat_id}"
                        )
                        result["errors"].append(
                            f"Failed to create chat {external_chat_id}"
                        )
                        return result

                    chat_id = new_chat["id"]
                    result["chats_created"] = 1
                    result["messages_loaded"] = len(avito_messages)
            else:
                metadata = {}
                context = avito_chat.get("context", {})
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

                if existing_chat.get("chat_contact_id"):
                    from database.db import chat_contacts

                    contact_update = {}
                    existing_contact = None

                    if user_name:
                        contact_update["name"] = user_name
                    if user_phone:
                        contact_update["phone"] = user_phone
                    if user_avatar:
                        contact_update["avatar"] = user_avatar

                    if client_user_id:
                        existing_contact = await database.fetch_one(
                            chat_contacts.select().where(
                                chat_contacts.c.id == existing_chat["chat_contact_id"]
                            )
                        )
                        if existing_contact and (
                            not existing_contact.get("external_contact_id")
                            or existing_contact.get("external_contact_id")
                            != str(client_user_id)
                        ):
                            contact_update["external_contact_id"] = str(client_user_id)

                    if contact_update:
                        await database.execute(
                            chat_contacts.update()
                            .where(
                                chat_contacts.c.id == existing_chat["chat_contact_id"]
                            )
                            .values(**contact_update)
                        )
                elif (user_name or user_phone) and existing_chat["id"]:
                    from database.db import chat_contacts

                    contact_data = {
                        "channel_id": channel_id,
                        "external_contact_id": (
                            str(client_user_id) if client_user_id else None
                        ),
                        "name": user_name,
                        "phone": user_phone,
                        "avatar": user_avatar,
                    }
                    contact_result = await database.fetch_one(
                        chat_contacts.insert()
                        .values(**contact_data)
                        .returning(chat_contacts.c.id)
                    )
                    if contact_result:
                        await database.execute(
                            chats.update()
                            .where(chats.c.id == existing_chat["id"])
                            .values(chat_contact_id=contact_result["id"])
                        )

                chat_update = {}
                if metadata:
                    chat_update["metadata"] = metadata

                last_message = avito_chat.get("last_message")
                if last_message and last_message.get("created"):
                    last_message_time = datetime.fromtimestamp(last_message["created"])
                    chat_update["last_message_time"] = last_message_time

                if chat_update:
                    await database.execute(
                        chats.update()
                        .where(chats.c.id == existing_chat["id"])
                        .values(**chat_update)
                    )

                result["chats_updated"] = 1
                chat_id = existing_chat["id"]

                messages_since = from_date if use_date_filter else None
                try:
                    avito_messages = await client.sync_messages(
                        chat_id=external_chat_id, since_timestamp=messages_since
                    )

                    result["messages_loaded"] = len(avito_messages)
                except Exception as sync_error:
                    error_str = str(sync_error)
                    if (
                        "402" in error_str
                        or "подписку" in error_str.lower()
                        or "subscription" in error_str.lower()
                    ):
                        avito_messages = []
                        last_message = avito_chat.get("last_message")
                        if last_message and isinstance(last_message, dict):
                            try:
                                from database.db import chat_messages

                                external_message_id = last_message.get("id")
                                if external_message_id:
                                    existing_message = await database.fetch_one(
                                        chat_messages.select().where(
                                            (
                                                chat_messages.c.external_message_id
                                                == external_message_id
                                            )
                                            & (chat_messages.c.chat_id == chat_id)
                                        )
                                    )

                                    if not existing_message:
                                        content = last_message.get("content", {})
                                        message_type_str = last_message.get(
                                            "type", "text"
                                        )
                                        message_text = ""

                                        if isinstance(content, dict):
                                            if message_type_str == "text":
                                                message_text = content.get("text", "")
                                            elif message_type_str == "system":
                                                message_text = content.get(
                                                    "text", "[Системное сообщение]"
                                                )
                                            elif message_type_str == "image":
                                                message_text = "[Изображение]"
                                            else:
                                                message_text = f"[{message_type_str}]"

                                        direction = last_message.get("direction", "in")
                                        sender_type = (
                                            "CLIENT"
                                            if direction == "in"
                                            else "OPERATOR"
                                        )

                                        created_timestamp = last_message.get("created")
                                        created_at = None
                                        if created_timestamp:
                                            created_at = datetime.fromtimestamp(
                                                created_timestamp
                                            )

                                        is_read = (
                                            last_message.get("is_read", False)
                                            or last_message.get("read") is not None
                                        )
                                        status = "READ" if is_read else "DELIVERED"

                                        db_message = await crud.create_message_and_update_chat(
                                            chat_id=chat_id,
                                            sender_type=sender_type,
                                            content=message_text
                                            or f"[{message_type_str}]",
                                            message_type=AvitoHandler._map_message_type(
                                                message_type_str
                                            ),
                                            external_message_id=external_message_id,
                                            status=status,
                                            source="avito",
                                            created_at=created_at,
                                        )
                                        db_message_id = (
                                            db_message.get("id")
                                            if isinstance(db_message, dict)
                                            else db_message.id
                                        )
                                        result["messages_created"] += 1
                                        result["messages_loaded"] += 1
                            except Exception as e:
                                logger.warning(
                                    f"Failed to create message from last_message for chat {external_chat_id}: {e}"
                                )
                    else:
                        logger.warning(
                            f"Failed to sync messages for chat {external_chat_id}: {sync_error}"
                        )
                        result["errors"].append(
                            f"Failed to sync messages for chat {external_chat_id}: {str(sync_error)}"
                        )
                        avito_messages = []

            if chat_id and avito_messages:
                for msg_idx, avito_msg in enumerate(avito_messages):
                    try:
                        external_message_id = avito_msg.get("id")
                        if not external_message_id:
                            continue

                        from database.db import chat_messages

                        existing_message = await database.fetch_one(
                            chat_messages.select().where(
                                (
                                    chat_messages.c.external_message_id
                                    == external_message_id
                                )
                                & (chat_messages.c.chat_id == chat_id)
                            )
                        )

                        content = avito_msg.get("content", {})
                        message_type_str = avito_msg.get("type", "text")
                        direction = avito_msg.get("direction", "in")
                        created_timestamp = avito_msg.get("created")

                        if message_type_str == "deleted":
                            continue

                        message_text_preview = ""
                        if isinstance(content, dict):
                            message_text_preview = content.get("text", "")
                        elif isinstance(content, str):
                            message_text_preview = content

                        if message_text_preview:
                            message_text_lower = message_text_preview.lower().strip()
                            if (
                                message_text_lower == "[deleted]"
                                or message_text_lower == "сообщение удалено"
                                or "[deleted]" in message_text_lower
                            ):
                                continue

                        if existing_message:
                            result["messages_updated"] += 1

                            if message_type_str in [
                                "image",
                                "voice",
                                "video",
                                "file",
                                "document",
                            ]:
                                from database.db import pictures

                                existing_picture = await database.fetch_one(
                                    pictures.select().where(
                                        (pictures.c.entity == "messages")
                                        & (
                                            pictures.c.entity_id
                                            == existing_message["id"]
                                        )
                                    )
                                )
                                if not existing_picture:
                                    file_url = (
                                        AvitoHandler._extract_message_media_file_url(
                                            content, message_type_str
                                        )
                                    )
                                    if (
                                        not file_url
                                        and message_type_str == "voice"
                                        and isinstance(content, dict)
                                    ):
                                        voice_id = AvitoHandler._extract_voice_id(
                                            content
                                        )
                                        if voice_id:
                                            try:
                                                file_url = (
                                                    await client.get_voice_file_url(
                                                        voice_id
                                                    )
                                                )
                                            except Exception as e:
                                                logger.warning(
                                                    f"Failed to get voice URL for voice_id {voice_id}: {e}"
                                                )

                                    if file_url:
                                        await database.execute(
                                            pictures.insert().values(
                                                entity="messages",
                                                entity_id=existing_message["id"],
                                                url=file_url,
                                                is_main=False,
                                                is_deleted=False,
                                                owner=cashbox_id,
                                                cashbox=cashbox_id,
                                            )
                                        )

                            continue

                        _, message_text = AvitoHandler._extract_message_content(
                            content, message_type_str
                        )
                        if not message_text:
                            if isinstance(content, str) and content:
                                message_text = content
                            else:
                                message_text = f"[{message_type_str}]"

                        sender_type = "CLIENT" if direction == "in" else "OPERATOR"

                        is_read = (
                            avito_msg.get("is_read", False)
                            or avito_msg.get("read") is not None
                        )
                        status = "READ" if is_read else "DELIVERED"

                        created_at = None
                        if created_timestamp:
                            created_at = datetime.fromtimestamp(created_timestamp)

                        db_message = await crud.create_message_and_update_chat(
                            chat_id=chat_id,
                            sender_type=sender_type,
                            content=message_text or f"[{message_type_str}]",
                            message_type=AvitoHandler._map_message_type(
                                message_type_str
                            ),
                            external_message_id=external_message_id,
                            status=status,
                            source="avito",
                            created_at=created_at,
                        )
                        db_message_id = (
                            db_message.get("id")
                            if isinstance(db_message, dict)
                            else db_message.id
                        )
                        result["messages_created"] += 1

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

                                    file_url = (
                                        AvitoHandler._extract_message_media_file_url(
                                            content, message_type_str
                                        )
                                    )
                                    if (
                                        not file_url
                                        and message_type_str == "voice"
                                        and isinstance(content, dict)
                                    ):
                                        voice_id = AvitoHandler._extract_voice_id(
                                            content
                                        )
                                        if voice_id:
                                            try:
                                                file_url = (
                                                    await client.get_voice_file_url(
                                                        voice_id
                                                    )
                                                )
                                            except Exception as e:
                                                logger.warning(
                                                    f"Failed to get voice URL for voice_id {voice_id}: {e}"
                                                )

                                    if file_url:
                                        message_id = db_message_id
                                        await database.execute(
                                            pictures.insert().values(
                                                entity="messages",
                                                entity_id=message_id,
                                                url=file_url,
                                                is_main=False,
                                                is_deleted=False,
                                                owner=cashbox_id,
                                                cashbox=cashbox_id,
                                            )
                                        )
                                except Exception as e:
                                    logger.warning(
                                        f"Failed to save {message_type_str} file for message {external_message_id}: {e}"
                                    )

                    except Exception as e:
                        logger.warning(
                            f"Failed to save message {avito_msg.get('id')}: {e}"
                        )
                        result["errors"].append(
                            f"Failed to save message {avito_msg.get('id')}: {str(e)}"
                        )

                if result["messages_loaded"] > 0:
                    last_db_message = await database.fetch_one(
                        chat_messages.select()
                        .where(chat_messages.c.chat_id == chat_id)
                        .order_by(desc(chat_messages.c.created_at))
                        .limit(1)
                    )
                    if last_db_message:
                        last_msg_time = (
                            last_db_message.get("created_at")
                            if isinstance(last_db_message, dict)
                            else last_db_message.created_at
                        )
                        if last_msg_time:
                            await database.execute(
                                chats.update()
                                .where(chats.c.id == chat_id)
                                .values(updated_at=last_msg_time)
                            )

        except Exception as e:
            error_str = str(e)
            if (
                "402" in error_str
                or "подписку" in error_str.lower()
                or "subscription" in error_str.lower()
            ):
                pass
            else:
                logger.warning(f"Failed to process chat {avito_chat.get('id')}: {e}")
                result["errors"].append(
                    f"Failed to process chat {avito_chat.get('id')}: {str(e)}"
                )

        return result


@router.post("/history/load", response_model=AvitoHistoryLoadResponse)
async def load_avito_history(
    channel_id: int = Query(..., description="ID канала Avito"),
    from_date: int = Query(
        ..., description="Unix timestamp, начиная с которого загружать историю"
    ),
    user=Depends(get_current_user),
):
    try:
        cashbox_id = user.cashbox_id

        avito_channel = await crud.get_channel(channel_id)
        if not avito_channel:
            raise HTTPException(
                status_code=404, detail=f"Channel {channel_id} not found"
            )

        if avito_channel.get("type") != "AVITO":
            raise HTTPException(
                status_code=400, detail=f"Channel {channel_id} is not an Avito channel"
            )

        channel_creds = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == channel_id)
                & (channel_credentials.c.cashbox_id == cashbox_id)
                & (channel_credentials.c.is_active.is_(True))
            )
        )

        if not channel_creds:
            raise HTTPException(
                status_code=403,
                detail=f"Channel {channel_id} does not belong to cashbox {cashbox_id}",
            )

        client = await create_avito_client(
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                channel_id, cashbox_id, token_data
            ),
        )

        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials.",
            )

        chats_processed = 0
        chats_created = 0
        chats_updated = 0
        messages_loaded = 0
        messages_created = 0
        messages_updated = 0
        errors = []

        all_chats = []
        offset = 0
        limit = 100
        max_offset = 1000

        current_timestamp = int(datetime.now().timestamp())

        use_date_filter = True
        if from_date > current_timestamp:
            use_date_filter = False

        while offset < max_offset:
            try:
                avito_chats = await client.get_chats(
                    limit=limit, offset=offset, chat_types=["u2i", "u2u"]
                )

                if not avito_chats:
                    break

                if use_date_filter:
                    filtered_chats = []
                    for chat in avito_chats:
                        chat_id = chat.get("id")
                        chat_created = chat.get("created", 0)
                        last_message = chat.get("last_message", {})
                        last_message_created = (
                            last_message.get("created", 0)
                            if isinstance(last_message, dict)
                            else 0
                        )

                        if (
                            chat_created >= from_date
                            or last_message_created >= from_date
                        ):
                            filtered_chats.append(chat)

                    all_chats.extend(filtered_chats)
                else:
                    all_chats.extend(avito_chats)

                if len(avito_chats) < limit:
                    if avito_chats:
                        last_chat = avito_chats[-1]
                        last_chat_id = last_chat.get("id", "unknown")
                        last_chat_created = last_chat.get("created", 0)
                        last_message = last_chat.get("last_message", {})
                        last_msg_created = (
                            last_message.get("created", 0)
                            if isinstance(last_message, dict)
                            else 0
                        )
                        last_chat_created_str = (
                            datetime.fromtimestamp(last_chat_created).isoformat()
                            if last_chat_created
                            else "N/A"
                        )
                        last_msg_created_str = (
                            datetime.fromtimestamp(last_msg_created).isoformat()
                            if last_msg_created
                            else "N/A"
                        )
                    break

                offset += limit

                if avito_chats:
                    last_chat = avito_chats[-1]
                    last_chat_id = last_chat.get("id", "unknown")
                    last_chat_created = last_chat.get("created", 0)
                    last_message = last_chat.get("last_message", {})
                    last_msg_created = (
                        last_message.get("created", 0)
                        if isinstance(last_message, dict)
                        else 0
                    )
                    last_chat_created_str = (
                        datetime.fromtimestamp(last_chat_created).isoformat()
                        if last_chat_created
                        else "N/A"
                    )
                    last_msg_created_str = (
                        datetime.fromtimestamp(last_msg_created).isoformat()
                        if last_msg_created
                        else "N/A"
                    )

                if avito_chats:
                    last_chat = avito_chats[-1]
                    last_chat_id = last_chat.get("id", "unknown")
                    last_chat_created = last_chat.get("created", 0)
                    last_message = last_chat.get("last_message", {})
                    last_msg_created = (
                        last_message.get("created", 0)
                        if isinstance(last_message, dict)
                        else 0
                    )
                    last_chat_created_str = (
                        datetime.fromtimestamp(last_chat_created).isoformat()
                        if last_chat_created
                        else "N/A"
                    )
                    last_msg_created_str = (
                        datetime.fromtimestamp(last_msg_created).isoformat()
                        if last_msg_created
                        else "N/A"
                    )

            except Exception as e:
                error_str = str(e)

                if "400" in error_str:
                    errors.append(f"Error at offset {offset}: {str(e)}")
                    break

                if offset >= max_offset:
                    break

                if (
                    "402" in error_str
                    or "подписку" in error_str.lower()
                    or "subscription" in error_str.lower()
                ):
                    errors.append(f"Subscription error at offset {offset}: {str(e)}")
                    offset += limit
                    continue

                logger.error(f"Error loading chats at offset {offset}: {e}")
                errors.append(f"Error loading chats at offset {offset}: {str(e)}")
                offset += limit
                continue

        max_concurrent = 10
        semaphore = asyncio.Semaphore(max_concurrent)

        tasks = [
            process_single_chat(
                avito_chat=avito_chat,
                client=client,
                channel_id=channel_id,
                cashbox_id=cashbox_id,
                from_date=from_date,
                use_date_filter=use_date_filter,
                semaphore=semaphore,
            )
            for avito_chat in all_chats
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for idx, result in enumerate(results, 1):
            if isinstance(result, Exception):
                error_str = str(result)
                logger.error(f"Error processing chat at index {idx}: {result}")
                errors.append(f"Error processing chat at index {idx}: {error_str}")
                continue

            chats_processed += result.get("chats_processed", 0)
            chats_created += result.get("chats_created", 0)
            chats_updated += result.get("chats_updated", 0)
            messages_loaded += result.get("messages_loaded", 0)
            messages_created += result.get("messages_created", 0)
            messages_updated += result.get("messages_updated", 0)

            if result.get("errors"):
                errors.extend(result["errors"])

        return AvitoHistoryLoadResponse(
            success=True,
            channel_id=channel_id,
            from_date=from_date,
            chats_processed=chats_processed,
            chats_created=chats_created,
            chats_updated=chats_updated,
            messages_loaded=messages_loaded,
            messages_created=messages_created,
            messages_updated=messages_updated,
            errors=errors if errors else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"History load error: {e}")
        raise HTTPException(status_code=400, detail=f"History load error: {str(e)}")


@router.post("/chats/{chat_id}/mark-read")
async def mark_avito_chat_as_read(
    chat_id: str,
    user=Depends(get_current_user),
):
    try:
        cashbox_id = user.cashbox_id

        chat = None
        try:
            internal_chat_id = int(chat_id)
            chat = await crud.get_chat(internal_chat_id)
        except ValueError:
            query = (
                select(
                    [
                        chats.c.id,
                        chats.c.channel_id,
                        chats.c.cashbox_id,
                        chats.c.external_chat_id,
                    ]
                )
                .select_from(chats.join(channels, chats.c.channel_id == channels.c.id))
                .where(
                    and_(
                        channels.c.type == "AVITO",
                        channels.c.is_active.is_(True),
                        chats.c.external_chat_id == chat_id,
                        chats.c.cashbox_id == cashbox_id,
                    )
                )
                .limit(1)
            )
            chat_result = await database.fetch_one(query)
            if chat_result:
                chat = await crud.get_chat(chat_result["id"])

        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")

        if chat["cashbox_id"] != cashbox_id:
            raise HTTPException(
                status_code=403,
                detail="Access denied - chat belongs to another cashbox",
            )

        if not chat.get("external_chat_id"):
            raise HTTPException(status_code=400, detail="Chat has no external_chat_id")

        avito_channel = await crud.get_channel(chat["channel_id"])
        if not avito_channel or avito_channel.get("type") != "AVITO":
            raise HTTPException(
                status_code=400, detail="Chat is not from Avito channel"
            )

        client = await create_avito_client(
            channel_id=chat["channel_id"],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                chat["channel_id"], cashbox_id, token_data
            ),
        )

        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials.",
            )

        success = await client.mark_chat_as_read(chat["external_chat_id"])

        if success:
            try:
                update_query = (
                    update(chat_messages)
                    .where(
                        and_(
                            chat_messages.c.chat_id == chat["id"],
                            chat_messages.c.sender_type == "CLIENT",
                            chat_messages.c.status != "READ",
                        )
                    )
                    .values(status="READ")
                )
                await database.execute(update_query)
            except Exception as e:
                logger.warning(f"Failed to update message statuses: {e}")

            try:
                from datetime import datetime

                from api.chats.websocket import cashbox_manager

                ws_message = {
                    "type": "chat_message",
                    "event": "message_read",
                    "chat_id": chat["id"],
                    "timestamp": datetime.utcnow().isoformat(),
                }
                await cashbox_manager.broadcast_to_cashbox(cashbox_id, ws_message)
            except Exception as e:
                logger.warning(f"Failed to send WebSocket event for chat read: {e}")

            return {
                "success": True,
                "message": f"Chat {chat['external_chat_id']} marked as read",
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to mark chat as read")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking chat as read: {e}")
        raise HTTPException(
            status_code=400, detail=f"Error marking chat as read: {str(e)}"
        )


@router.get("/webhooks/list")
async def get_avito_webhooks(user=Depends(get_current_user)):
    try:
        cashbox_id = user.cashbox_id

        avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")

        if not avito_channel:
            raise HTTPException(
                status_code=400,
                detail="Avito channel not configured for this cashbox. Please connect via /connect endpoint first.",
            )

        client = await create_avito_client(
            channel_id=avito_channel["id"],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                avito_channel["id"], cashbox_id, token_data
            ),
        )

        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials.",
            )

        webhooks = await client.get_webhooks()

        return {"success": True, "webhooks": webhooks, "count": len(webhooks)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting webhooks: {e}")
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")


@router.post("/webhooks/register")
async def register_avito_webhook(
    webhook_url: Optional[str] = None,
    user=Depends(get_current_user),
):
    try:
        cashbox_id = user.cashbox_id

        avito_channel = await crud.get_channel_by_cashbox(cashbox_id, "AVITO")

        if not avito_channel:
            raise HTTPException(
                status_code=400,
                detail="Avito channel not configured for this cashbox. Please connect via /connect endpoint first.",
            )

        credentials = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == avito_channel["id"])
                & (channel_credentials.c.cashbox_id == cashbox_id)
                & (channel_credentials.c.is_active.is_(True))
            )
        )

        if not credentials:
            raise HTTPException(
                status_code=404,
                detail=f"Avito credentials not found for cashbox {cashbox_id}. Please connect Avito channel first via /connect endpoint.",
            )

        if not credentials.get("access_token"):
            raise HTTPException(
                status_code=400,
                detail="Avito channel not authorized. Please complete OAuth authorization first.",
            )

        if not webhook_url:
            webhook_url = f"{_get_avito_app_url()}/api/v1/avito/hook"

        client = await create_avito_client(
            channel_id=avito_channel["id"],
            cashbox_id=cashbox_id,
            on_token_refresh=lambda token_data: save_token_callback(
                avito_channel["id"], cashbox_id, token_data
            ),
        )

        if not client:
            raise HTTPException(
                status_code=500,
                detail="Could not create Avito API client. Check credentials.",
            )

        try:
            result = await client.register_webhook(webhook_url)
            return {
                "success": True,
                "message": "Webhook registered successfully",
                "webhook_url": webhook_url,
                "result": result,
            }
        except Exception as webhook_error:
            logger.error(f"Error registering webhook: {webhook_error}")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to register webhook: {str(webhook_error)}",
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering webhook: {e}")
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")


@router.get("/oauth/callback", response_model=AvitoOAuthCallbackResponse)
async def avito_oauth_callback(
    code: str = Query(..., description="Authorization code from Avito"),
    state: str = Query(..., description="State parameter for CSRF protection"),
    token: Optional[str] = Query(
        None, description="Optional user authentication token"
    ),
):
    try:
        # 1. Извлекаем cashbox_id из state
        try:
            parts = state.split("_", 1)
            if len(parts) != 2:
                raise ValueError(
                    "State format invalid: expected cashbox_id_state_token"
                )
            cashbox_id_str, state_token = parts
            cashbox_id = int(cashbox_id_str)
        except (ValueError, IndexError):
            raise HTTPException(status_code=400, detail="Invalid state parameter")

        # 2. Получаем redirect_uri (из любых активных credentials кассы или стандартный)
        any_creds = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.cashbox_id == cashbox_id)
                & (channel_credentials.c.is_active.is_(True))
            )
        )
        redirect_uri = (
            any_creds.get("redirect_uri") if any_creds else None
        ) or f"{_get_avito_app_url()}/api/v1/hook/chat/123456"

        # 3. Обмениваем код на токены
        token_data = await AvitoClient.exchange_authorization_code_for_tokens(
            client_id=AVITO_OAUTH_CLIENT_ID,
            client_secret=AVITO_OAUTH_CLIENT_SECRET,
            authorization_code=code,
            redirect_uri=redirect_uri,
        )
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        expires_at_str = token_data.get("expires_at")
        token_expires_at = (
            datetime.fromisoformat(expires_at_str) if expires_at_str else None
        )
        if not access_token:
            raise HTTPException(
                status_code=500, detail="Failed to obtain access token from Avito OAuth"
            )

        # 4. Получаем avito_user_id и имя аккаунта
        temp_client = AvitoClient(
            api_key=AVITO_OAUTH_CLIENT_ID,
            api_secret=AVITO_OAUTH_CLIENT_SECRET,
            access_token=access_token,
            refresh_token=refresh_token,
            token_expires_at=token_expires_at,
        )
        try:
            avito_user_id = await temp_client._get_user_id()
            user_profile = await temp_client.get_user_profile()
            avito_account_name = user_profile.get("name") or f"Cashbox {cashbox_id}"
        except Exception as e:
            avito_account_name = f"Cashbox {cashbox_id}"
            logger.warning(f"Failed to get avito_user_id or user profile: {e}")
            raise HTTPException(
                status_code=400,
                detail="Не удалось получить avito_user_id из профиля пользователя",
            )

        # 5. Шифруем токены
        encrypted_access_token = _encrypt_credential(access_token)
        encrypted_refresh_token = (
            _encrypt_credential(refresh_token) if refresh_token else None
        )
        encrypted_oauth_api_key = _encrypt_credential(AVITO_OAUTH_CLIENT_ID)
        encrypted_oauth_api_secret = _encrypt_credential(AVITO_OAUTH_CLIENT_SECRET)

        existing_channel_query = (
            select([channels.c.id])
            .where(
                and_(
                    channels.c.cashbox_id == cashbox_id,
                    channels.c.type == "AVITO",
                    channels.c.is_active.is_(True),
                )
            )
            .limit(1)
        )
        existing_channel_row = await database.fetch_one(existing_channel_query)

        if existing_channel_row:
            channel_id = existing_channel_row["id"]
            # Ищем credentials для этого канала и кассы
            existing_creds = await database.fetch_one(
                channel_credentials.select().where(
                    (channel_credentials.c.channel_id == channel_id)
                    & (channel_credentials.c.cashbox_id == cashbox_id)
                )
            )
            if existing_creds:
                # Обновляем токены и avito_user_id
                await database.execute(
                    channel_credentials.update()
                    .where(channel_credentials.c.id == existing_creds["id"])
                    .values(
                        access_token=encrypted_access_token,
                        refresh_token=encrypted_refresh_token,
                        token_expires_at=token_expires_at,
                        avito_user_id=avito_user_id,  # на случай, если раньше отсутствовал
                        updated_at=datetime.utcnow(),
                        is_active=True,
                    )
                )
            else:
                # Создаём credentials для существующего канала
                await database.execute(
                    channel_credentials.insert().values(
                        channel_id=channel_id,
                        cashbox_id=cashbox_id,
                        api_key=encrypted_oauth_api_key,
                        api_secret=encrypted_oauth_api_secret,
                        redirect_uri=redirect_uri,
                        access_token=encrypted_access_token,
                        refresh_token=encrypted_refresh_token,
                        token_expires_at=token_expires_at,
                        avito_user_id=avito_user_id,
                        is_active=True,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                    )
                )
        else:
            # Канала ещё нет – создаём новый
            from api.chats.avito.avito_constants import AVITO_SVG_ICON

            channel_name = f"Avito - {avito_account_name} ({avito_user_id})"
            channel_id = await database.execute(
                channels.insert().values(
                    name=channel_name,
                    type="AVITO",
                    cashbox_id=cashbox_id,
                    svg_icon=AVITO_SVG_ICON,
                    description=f"Avito OAuth Integration for Cashbox {cashbox_id}, User {avito_user_id}",
                    is_active=True,
                )
            )
            await database.execute(
                channel_credentials.insert().values(
                    channel_id=channel_id,
                    cashbox_id=cashbox_id,
                    api_key=encrypted_oauth_api_key,
                    api_secret=encrypted_oauth_api_secret,
                    redirect_uri=redirect_uri,
                    access_token=encrypted_access_token,
                    refresh_token=encrypted_refresh_token,
                    token_expires_at=token_expires_at,
                    avito_user_id=avito_user_id,
                    is_active=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
            )
            logger.info(
                f"Created new channel_id={channel_id} for avito_user_id={avito_user_id}"
            )

        # 7. Регистрируем вебхук с явной передачей cashbox_id в URL
        webhook_registered = False
        webhook_error_message = None
        webhook_url = None
        try:
            webhook_url = f"{_get_avito_app_url()}/api/v1/avito/hook/{cashbox_id}"
            client = await create_avito_client(
                channel_id=channel_id,
                cashbox_id=cashbox_id,
                on_token_refresh=lambda td: save_token_callback(
                    channel_id, cashbox_id, td
                ),
            )
            if client:
                try:
                    await client.register_webhook(webhook_url)
                    webhook_registered = True
                except Exception as e:
                    webhook_error_message = str(e)
            else:
                webhook_error_message = "Could not create Avito client"
        except Exception as e:
            webhook_error_message = str(e)

        # 8. Формируем ответ
        response_data = {
            "success": True,
            "message": f"Avito канал успешно подключен через OAuth к кабинету {cashbox_id}",
            "channel_id": channel_id,
            "cashbox_id": cashbox_id,
        }
        if webhook_registered:
            response_data["webhook_registered"] = True
            response_data["webhook_url"] = webhook_url
        elif webhook_error_message:
            response_data["webhook_registered"] = False
            response_data["webhook_error"] = webhook_error_message

        return AvitoOAuthCallbackResponse(**response_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in OAuth callback: {e}")
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")
