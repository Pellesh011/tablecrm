import logging
import re
from datetime import datetime
from typing import Optional

from api.chats import crud
from api.chats.avito.avito_factory import create_avito_client, save_token_callback
from api.chats.avito.avito_handler import AvitoHandler
from common.decorators import ensure_db_connection
from database.db import channel_credentials, channels, chat_messages, chats, database
from sqlalchemy import and_, select

logger = logging.getLogger(__name__)


def extract_phone_from_text(text: str) -> Optional[str]:
    """Извлечь телефон из текста"""
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


@ensure_db_connection
async def sync_avito_chats_and_messages():
    """
    Автоматическая выгрузка новых чатов и сообщений из Avito каждые 5 минут.
    Работает только для аккаунтов с auto_sync_chats_enabled = true.
    """

    # Получаем все активные credentials для Avito каналов с включенной автоматической синхронизацией
    query = (
        select(
            [
                channel_credentials.c.id,
                channel_credentials.c.channel_id,
                channel_credentials.c.cashbox_id,
                channel_credentials.c.avito_user_id,
            ]
        )
        .select_from(
            channel_credentials.join(
                channels, channel_credentials.c.channel_id == channels.c.id
            )
        )
        .where(
            and_(
                channels.c.type == "AVITO",
                channels.c.is_active.is_(True),
                channel_credentials.c.is_active.is_(True),
                channel_credentials.c.auto_sync_chats_enabled.is_(True),
            )
        )
    )

    all_credentials = await database.fetch_all(query)

    if not all_credentials:
        logger.info("No Avito credentials with auto_sync_chats_enabled found")
        return

    logger.info(f"Starting auto-sync for {len(all_credentials)} Avito accounts")

    total_chats_created = 0
    total_chats_updated = 0
    total_messages_loaded = 0
    total_errors = 0

    for cred in all_credentials:
        try:
            channel_id = cred["channel_id"]
            cashbox_id = cred["cashbox_id"]

            logger.info(
                f"Processing auto-sync for channel {channel_id}, cashbox {cashbox_id}"
            )

            # Создаем клиент
            client = await create_avito_client(
                channel_id=channel_id,
                cashbox_id=cashbox_id,
                on_token_refresh=lambda token_data, ch_id=channel_id, cb_id=cashbox_id: save_token_callback(
                    ch_id, cb_id, token_data
                ),
            )

            if not client:
                logger.warning(
                    f"Could not create Avito client for channel {channel_id}, cashbox {cashbox_id}"
                )
                total_errors += 1
                continue

            # Получаем чаты с пагинацией
            offset = 0
            limit = 100
            all_avito_chats = []

            while True:
                try:
                    avito_chats = await client.get_chats(
                        limit=limit, offset=offset, unread_only=False
                    )
                    if not avito_chats:
                        break

                    all_avito_chats.extend(avito_chats)

                    if len(avito_chats) < limit:
                        break

                    offset += limit
                except Exception as e:
                    error_str = str(e)
                    if (
                        "402" in error_str
                        or "подписку" in error_str.lower()
                        or "subscription" in error_str.lower()
                    ):
                        logger.warning(f"Subscription required for loading chats: {e}")
                        break
                    logger.error(f"Failed to get chats at offset {offset}: {e}")
                    total_errors += 1
                    break

            logger.info(
                f"Retrieved {len(all_avito_chats)} chats from Avito API for channel {channel_id}"
            )

            # Получаем avito_user_id из credentials
            creds = await database.fetch_one(
                channel_credentials.select().where(
                    (channel_credentials.c.channel_id == channel_id)
                    & (channel_credentials.c.cashbox_id == cashbox_id)
                    & (channel_credentials.c.is_active.is_(True))
                )
            )
            avito_user_id = creds.get("avito_user_id") if creds else None

            chats_created = 0
            chats_updated = 0

            # Обрабатываем каждый чат
            for avito_chat in all_avito_chats:
                try:
                    external_chat_id = avito_chat.get("id")
                    if not external_chat_id:
                        continue

                    # Извлекаем информацию о пользователе
                    users = avito_chat.get("users", [])
                    user_name = None
                    user_phone = None
                    user_avatar = None
                    client_user_id = None

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
                                    or user.get("public_user_profile", {}).get(
                                        "phone_number"
                                    )
                                )
                                public_profile = user.get("public_user_profile", {})
                                if public_profile:
                                    avatar_data = public_profile.get("avatar", {})
                                    if isinstance(avatar_data, dict):
                                        user_avatar = (
                                            avatar_data.get("default")
                                            or avatar_data.get("images", {}).get(
                                                "256x256"
                                            )
                                            or avatar_data.get("images", {}).get(
                                                "128x128"
                                            )
                                            or (
                                                list(
                                                    avatar_data.get(
                                                        "images", {}
                                                    ).values()
                                                )[0]
                                                if avatar_data.get("images")
                                                else None
                                            )
                                        )
                                    elif isinstance(avatar_data, str):
                                        user_avatar = avatar_data
                                if user_name or user_phone:
                                    break

                    # Пытаемся извлечь телефон из последнего сообщения
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

                    # Проверяем существующий чат
                    existing_chat = await database.fetch_one(
                        chats.select().where(
                            (chats.c.channel_id == channel_id)
                            & (chats.c.external_chat_id == external_chat_id)
                            & (chats.c.cashbox_id == cashbox_id)
                        )
                    )

                    chat_id = None

                    if existing_chat:
                        chat_id = existing_chat["id"]

                        context = avito_chat.get("context", {})
                        ad_title = None
                        ad_id = None
                        ad_url = None
                        if isinstance(context, dict):
                            item = context.get("item", {})
                            if isinstance(item, dict):
                                ad_title = item.get("title")
                                ad_id = item.get("id")
                                ad_url = item.get("url")

                        metadata = {}
                        if ad_title:
                            metadata["ad_title"] = ad_title
                        if ad_id:
                            metadata["ad_id"] = ad_id
                        if ad_url:
                            metadata["ad_url"] = ad_url
                        if context:
                            metadata["context"] = context

                        chat_contact_id = existing_chat.get("chat_contact_id")

                        if chat_contact_id:
                            from database.db import chat_contacts

                            contact_update = {}

                            if user_name:
                                contact_update["name"] = user_name
                            if user_phone:
                                contact_update["phone"] = user_phone
                            if user_avatar:
                                contact_update["avatar"] = user_avatar

                            if client_user_id:
                                existing_contact = await database.fetch_one(
                                    chat_contacts.select().where(
                                        chat_contacts.c.id == chat_contact_id
                                    )
                                )
                                if existing_contact and (
                                    not existing_contact.get("external_contact_id")
                                    or existing_contact.get("external_contact_id")
                                    != str(client_user_id)
                                ):
                                    contact_update["external_contact_id"] = str(
                                        client_user_id
                                    )

                            if contact_update:
                                await database.execute(
                                    chat_contacts.update()
                                    .where(chat_contacts.c.id == chat_contact_id)
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
                            last_message_time = datetime.fromtimestamp(
                                last_message["created"]
                            )
                            chat_update["last_message_time"] = last_message_time
                            chat_update["updated_at"] = last_message_time

                        if chat_update:
                            await database.execute(
                                chats.update()
                                .where(chats.c.id == chat_id)
                                .values(**chat_update)
                            )

                        chats_updated += 1
                        logger.debug(
                            f"Updated chat {chat_id} (external: {external_chat_id})"
                        )
                    else:
                        context = avito_chat.get("context", {})
                        ad_title = None
                        ad_id = None
                        ad_url = None
                        if isinstance(context, dict):
                            item = context.get("item", {})
                            if isinstance(item, dict):
                                ad_title = item.get("title")
                                ad_id = item.get("id")
                                ad_url = item.get("url")

                        metadata = {}
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

                        # Создаем новый чат через crud.create_chat
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
                            continue

                        chat_id = new_chat["id"]
                        chats_created += 1
                        logger.info(
                            f"Created new chat {chat_id} (external: {external_chat_id})"
                        )

                        if avito_chat.get("created"):
                            first_message_time = datetime.fromtimestamp(
                                avito_chat["created"]
                            )
                            await database.execute(
                                chats.update()
                                .where(chats.c.id == chat_id)
                                .values(first_message_time=first_message_time)
                            )

                        # Обновляем время последнего сообщения
                        last_message = avito_chat.get("last_message")
                        if last_message and last_message.get("created"):
                            last_message_time = datetime.fromtimestamp(
                                last_message["created"]
                            )
                            await database.execute(
                                chats.update()
                                .where(chats.c.id == chat_id)
                                .values(
                                    last_message_time=last_message_time,
                                    updated_at=last_message_time,
                                )
                            )

                    # Загружаем сообщения для чата
                    if chat_id:
                        try:
                            avito_messages = await client.sync_messages(
                                external_chat_id
                            )

                            messages_loaded = 0
                            for avito_msg in avito_messages:
                                try:
                                    external_message_id = avito_msg.get("id")
                                    if not external_message_id:
                                        continue

                                    # Проверяем существующее сообщение
                                    existing_message = await database.fetch_one(
                                        chat_messages.select().where(
                                            (
                                                chat_messages.c.external_message_id
                                                == external_message_id
                                            )
                                            & (chat_messages.c.chat_id == chat_id)
                                        )
                                    )

                                    if existing_message:
                                        continue

                                    # Извлекаем текст сообщения
                                    content = avito_msg.get("content", {})
                                    message_type_str = avito_msg.get("type", "text")
                                    message_text = ""

                                    if isinstance(content, dict):
                                        if message_type_str == "text":
                                            message_text = content.get("text", "")
                                        elif message_type_str == "link":
                                            link_data = content.get("link", {})
                                            message_text = link_data.get(
                                                "text",
                                                link_data.get("url", "[Ссылка]"),
                                            )
                                        elif message_type_str == "system":
                                            message_text = content.get(
                                                "text", "[Системное сообщение]"
                                            )
                                        elif message_type_str == "image":
                                            message_text = "[Изображение]"
                                        elif message_type_str == "item":
                                            item_data = content.get("item", {})
                                            message_text = f"Объявление: {item_data.get('title', '[Объявление]')}"
                                        elif message_type_str == "location":
                                            loc_data = content.get("location", {})
                                            message_text = loc_data.get(
                                                "text",
                                                loc_data.get("title", "[Геолокация]"),
                                            )
                                        elif message_type_str == "voice":
                                            message_text = "[Голосовое сообщение]"
                                        else:
                                            message_text = f"[{message_type_str}]"
                                    else:
                                        message_text = (
                                            str(content)
                                            if content
                                            else f"[{message_type_str}]"
                                        )

                                    direction = avito_msg.get("direction", "in")
                                    sender_type = (
                                        "CLIENT" if direction == "in" else "OPERATOR"
                                    )

                                    is_read = (
                                        avito_msg.get("is_read", False)
                                        or avito_msg.get("read") is not None
                                    )
                                    status = "READ" if is_read else "DELIVERED"

                                    created_timestamp = avito_msg.get("created")
                                    created_at = None
                                    if created_timestamp:
                                        created_at = datetime.fromtimestamp(
                                            created_timestamp
                                        )

                                    await crud.create_message_and_update_chat(
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
                                    messages_loaded += 1

                                except Exception as e:
                                    logger.warning(
                                        f"Failed to save message {avito_msg.get('id')}: {e}"
                                    )
                                    total_errors += 1

                            if messages_loaded > 0:
                                logger.info(
                                    f"Loaded {messages_loaded} messages for chat {chat_id} (external: {external_chat_id})"
                                )
                                total_messages_loaded += messages_loaded

                        except Exception as sync_error:
                            error_str = str(sync_error)
                            if (
                                "402" in error_str
                                or "подписку" in error_str.lower()
                                or "subscription" in error_str.lower()
                            ):
                                logger.warning(
                                    f"Subscription required for loading messages for chat {external_chat_id}"
                                )
                            else:
                                logger.error(
                                    f"Failed to sync messages for chat {chat_id}: {sync_error}"
                                )
                                total_errors += 1

                except Exception as e:
                    logger.warning(
                        f"Failed to process chat {avito_chat.get('id')}: {e}"
                    )
                    total_errors += 1

            total_chats_created += chats_created
            total_chats_updated += chats_updated

            logger.info(
                f"Auto-sync completed for channel {channel_id}, cashbox {cashbox_id}: "
                f"created={chats_created}, updated={chats_updated}"
            )

        except Exception as e:
            logger.error(
                f"Error processing auto-sync for credential {cred.get('id')}: {e}",
                exc_info=True,
            )
            total_errors += 1

    logger.info(
        f"Auto-sync job completed: chats_created={total_chats_created}, "
        f"chats_updated={total_chats_updated}, messages_loaded={total_messages_loaded}, errors={total_errors}"
    )
