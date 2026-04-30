"""
api/chats/max/max_handler.py
Обработчик входящих обновлений от Max Bot API.
"""

import base64
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp
from api.chats import crud
from api.chats.contact_service import (
    get_chat_contacts,
    get_contact_by_id,
    get_or_create_contact,
    link_contact_to_chat,
    update_contact_phone,
)
from api.chats.max.max_client import MaxClient
from api.chats.producer import chat_producer
from api.qr.routes import qr_visits
from database.db import (
    channel_credentials,
    chat_contact_links,
    chat_contacts,
    chat_messages,
    chats,
    database,
)

_AVATAR_CACHE: Dict[str, str] = {}


def _normalize_app_url(app_url: Optional[str]) -> Optional[str]:
    if not app_url:
        return None
    if not app_url.startswith("http://") and not app_url.startswith("https://"):
        app_url = f"https://{app_url}"
    return app_url.rstrip("/")


def _build_contact_name(sender: Dict[str, Any]) -> Optional[str]:
    name = sender.get("name", "").strip()
    username = sender.get("username", "").strip()
    return name or username or None


async def _download_url(url: str) -> Optional[bytes]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    return await resp.read()
    except Exception as exc:
        print(f"[Max] WARNING: download failed {url}: {exc}")
    return None


async def _get_or_upload_avatar(
    avatar_url_raw: Optional[str],
    user_id: int,
    cashbox_id: int,
    channel_id: int,
) -> Optional[str]:
    if not avatar_url_raw:
        return None
    print(f"[Max] Using avatar URL: {avatar_url_raw}")
    return avatar_url_raw


async def _prepare_file_bytes(file_url: str) -> Optional[bytes]:
    if not file_url:
        return None
    if file_url.startswith("data:"):
        try:
            header, encoded = file_url.split(",", 1)
            return base64.b64decode(encoded)
        except Exception as e:
            print(f"[Max] Failed to decode data URL: {e}")
            return None
    else:
        return await _download_url(file_url)


async def handle_update(
    update: Dict[str, Any],
    channel_id: int,
    cashbox_id: int,
    bot_token: str,
) -> Dict[str, Any]:
    update_type = update.get("update_type", "")
    print(
        f"[Max] HANDLE_UPDATE: type={update_type}, channel={channel_id}, cashbox={cashbox_id}"
    )
    print(f"[Max] Full update: {json.dumps(update, default=str, ensure_ascii=False)}")

    if update_type in ("message_created", "message_edited"):
        return await _handle_message(update, channel_id, cashbox_id, bot_token)

    if update_type == "bot_started":
        return await _handle_bot_started(update, channel_id, cashbox_id, bot_token)

    if update_type == "message_callback":
        return await _handle_message_callback(update, channel_id, cashbox_id, bot_token)

    if update_type in ("bot_added",):
        print(f"[Max] Unhandled event (ignored): {update_type}")
        return {"success": True, "message": f"{update_type} event received"}

    print(f"[Max] Unhandled update type: {update_type}")
    return {"success": True, "message": f"update_type={update_type} ignored"}


async def _handle_message_callback(
    update: Dict[str, Any],
    channel_id: int,
    cashbox_id: int,
    bot_token: str,
) -> Dict[str, Any]:
    callback = update.get("callback") or {}
    message = update.get("message") or {}
    payload = callback.get("payload", "")
    callback_id = callback.get("id")
    user = callback.get("user") or {}
    recipient = message.get("recipient") or {}

    print(f"[Max] Callback recipient: {recipient}")
    print(f"[Max] Callback user: {user}")

    chat_id_external = str(recipient.get("chat_id") or "")
    if not chat_id_external:
        chat_id_external = str(user.get("user_id", ""))
    if not chat_id_external:
        print("[Max] ERROR: Could not determine chat_id_external in callback")
        return {"success": False, "message": "No chat_id"}

    print(f"[Max] Callback chat_id_external = {chat_id_external}")

    existing_chat = await crud.get_chat_by_external_id(
        channel_id=channel_id,
        external_chat_id=chat_id_external,
        cashbox_id=cashbox_id,
    )
    if not existing_chat:
        sender_name = _build_contact_name(user) or f"Max User {user.get('user_id', 0)}"
        existing_chat = await crud.create_chat(
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            external_chat_id=chat_id_external,
            external_chat_id_for_contact=str(user.get("user_id")),
            name=sender_name,
            metadata={"source": "max", "max_user_id": user.get("user_id")},
        )
    chat_db_id = existing_chat["id"]

    last_activity = user.get("last_activity_time")
    contact_id = await get_or_create_contact(
        cashbox_id=cashbox_id,
        external_contact_id=str(user.get("user_id")),
        name=_build_contact_name(user),
        avatar=user.get("avatar_url"),
        phone=None,
        last_activity=last_activity,
    )
    await link_contact_to_chat(chat_db_id, contact_id)

    # Получаем телефон из контакта
    contact = await get_contact_by_id(contact_id)
    phone = contact.get("phone") if contact else None

    from api.chats.max.max_auto_reply import handle_callback

    handled = await handle_callback(
        bot_token=bot_token,
        payload=payload,
        channel_id=channel_id,
        cashbox_id=cashbox_id,
        chat_id=chat_db_id,
        max_chat_id=chat_id_external,
        max_user_id=user.get("user_id", 0),
        phone=phone,
        callback_id=callback_id,
        contact_id=contact_id,
    )

    existing_link = await database.fetch_one(
        chat_contact_links.select().where(
            (chat_contact_links.c.chat_id == chat_db_id)
            & (chat_contact_links.c.contact_id == contact_id)
        )
    )
    if not existing_link:
        await database.execute(
            chat_contact_links.insert().values(
                chat_id=chat_db_id,
                contact_id=contact_id,
                role="participant",
                created_at=datetime.utcnow(),
            )
        )

    if not handled:
        text = f"[Callback] {payload}"
        await crud.create_message_and_update_chat(
            chat_id=chat_db_id,
            sender_type="CLIENT",
            content=text,
            message_type="SYSTEM",
            status="DELIVERED",
            source="max",
        )

    return {"success": True}


async def _handle_message(
    update: Dict[str, Any],
    channel_id: int,
    cashbox_id: int,
    bot_token: str,
) -> Dict[str, Any]:
    print("[Max] ========== START _handle_message ==========")
    message = update.get("message") or {}
    if not message:
        print("[Max] ERROR: Empty message in update")
        return {"success": False, "message": "Empty message in update"}

    sender = message.get("sender") or {}
    recipient = message.get("recipient") or {}
    body = message.get("body") or {}
    timestamp_ms = message.get("timestamp") or update.get("timestamp") or 0

    sender_user_id: int = sender.get("user_id", 0)
    sender_name: Optional[str] = _build_contact_name(sender)
    sender_username: Optional[str] = sender.get("username")
    avatar_url_raw: Optional[str] = sender.get("avatar_url") or sender.get(
        "full_avatar_url"
    )

    chat_id_external: str = str(recipient.get("chat_id") or sender_user_id or "")
    chat_type: str = recipient.get("chat_type", "dialog")

    external_message_id: str = body.get("mid", "")
    message_text: str = body.get("text", "")
    attachments: List = body.get("attachments") or []

    if message.get("text", "").startswith("/start"):
        return {"success": True, "message": "Start command ignored for auto-reply"}

    print(
        f"[Max] external_id={external_message_id}, chat_external={chat_id_external}, user_id={sender_user_id}, text='{message_text[:50]}', attachments={len(attachments)}"
    )

    if not chat_id_external:
        print("[Max] ERROR: Cannot determine chat_id")
        return {"success": False, "message": "Cannot determine chat_id"}

    bot_user_id: Optional[int] = None
    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel_id)
            & (channel_credentials.c.cashbox_id == cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    if creds and creds.get("avito_user_id"):
        bot_user_id = creds["avito_user_id"]

    sender_type = (
        "OPERATOR" if (bot_user_id and sender_user_id == bot_user_id) else "CLIENT"
    )
    print(f"[Max] sender_type={sender_type}")

    avatar_url: Optional[str] = None
    if sender_type == "CLIENT" and sender_user_id:
        if not avatar_url_raw and chat_id_external:
            try:
                client = MaxClient(bot_token)
                chat_info = await client.get_chat(chat_id_external)
                dialog_user = chat_info.get("dialog_with_user")
                if dialog_user:
                    avatar_url_raw = dialog_user.get("avatar_url") or dialog_user.get(
                        "full_avatar_url"
                    )
                    print(f"[Max] Got avatar from chat info: {avatar_url_raw}")
            except Exception as e:
                print(f"[Max] Failed to fetch chat info for avatar: {e}")
        if avatar_url_raw:
            avatar_url = await _get_or_upload_avatar(
                avatar_url_raw, sender_user_id, cashbox_id, channel_id
            )
            print(f"[Max] Avatar URL: {avatar_url}")

    # Находим или создаём чат
    existing_chat = await crud.get_chat_by_external_id(
        channel_id=channel_id,
        external_chat_id=chat_id_external,
        cashbox_id=cashbox_id,
    )
    if existing_chat:
        chat = existing_chat
        print(f"[Max] Existing chat id={chat['id']}")
    else:
        chat = await crud.create_chat(
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            external_chat_id=chat_id_external,
            external_chat_id_for_contact=(
                str(sender_user_id) if sender_user_id else None
            ),
            name=sender_name,
            metadata={
                "source": "max",
                "username": sender_username,
                "max_user_id": sender_user_id,
                "chat_type": "dialog",
            },
        )
        print(f"[Max] Created new chat id={chat['id'] if chat else 'None'}")

    if not chat:
        print("[Max] ERROR: Failed to create or find chat")
        return {"success": False, "message": "Failed to create or find chat"}

    chat_db_id: int = chat["id"]
    print(f"[Max] chat_db_id={chat_db_id}")

    last_activity = sender.get("last_activity_time")
    contact_id = await get_or_create_contact(
        cashbox_id=cashbox_id,
        external_contact_id=str(sender_user_id),
        name=sender_name,
        avatar=avatar_url,
        phone=None,
        last_activity=last_activity,
    )
    await link_contact_to_chat(chat_db_id, contact_id)

    existing_link = await database.fetch_one(
        chat_contact_links.select().where(
            (chat_contact_links.c.chat_id == chat_db_id)
            & (chat_contact_links.c.contact_id == contact_id)
        )
    )
    if not existing_link:
        await database.execute(
            chat_contact_links.insert().values(
                chat_id=chat_db_id,
                contact_id=contact_id,
                role="participant",
                created_at=datetime.utcnow(),
            )
        )
    contact_attachments = []
    if attachments:
        for att in attachments:
            if att.get("type") == "contact":
                contact_attachments.append(att.get("payload", {}))

    for contact_payload in contact_attachments:
        phone = None
        vcf_info = contact_payload.get("vcf_info")
        if vcf_info:
            for line in vcf_info.splitlines():
                if line.startswith("TEL"):
                    parts = line.split(":")
                    if len(parts) > 1:
                        phone = parts[1].strip()
                        break
        if not phone:
            phone = contact_payload.get("phone")

        max_info = contact_payload.get("max_info", {})
        first_name = max_info.get("first_name")
        last_name = max_info.get("last_name")
        print(f"[Max] Processing contact: phone={phone}, name={first_name} {last_name}")
        if phone:
            # Обновляем телефон в контакте
            await update_contact_phone(contact_id, phone)

            from api.chats.max.max_auto_reply import handle_contact_received

            print("[Max] Calling handle_contact_received")
            await handle_contact_received(
                bot_token=bot_token,
                cashbox_id=cashbox_id,
                max_chat_id=chat_id_external,
                max_user_id=sender_user_id,
                phone=phone,
                first_name=first_name,
                last_name=last_name,
                chat_id=chat_db_id,
                contact_id=contact_id,
            )
            print("[Max] handle_contact_received finished")
        else:
            print("[Max] WARNING: Could not extract phone from contact attachment")

    created_at: Optional[datetime] = None
    if timestamp_ms:
        try:
            created_at = datetime.utcfromtimestamp(timestamp_ms / 1000)
        except Exception:
            pass
    attachment_items = []
    if attachments:
        for att in attachments:
            att_type = att.get("type", "")
            if att_type == "contact":
                continue
            payload = att.get("payload") or {}
            file_info = payload

            if not file_info.get("url") and file_info.get("photos"):
                photos = file_info.get("photos")
                for size in sorted(
                    photos.keys(), key=lambda x: int(x.split("x")[0]), reverse=True
                ):
                    url = (
                        photos[size].get("url")
                        if isinstance(photos[size], dict)
                        else photos[size]
                    )
                    if url:
                        file_info["url"] = url
                        break

            msg_type = "IMAGE"
            placeholder = "[Фото]"
            if att_type in ("image", "photo"):
                msg_type = "IMAGE"
                placeholder = "[Фото]"
            elif att_type in ("file", "document"):
                msg_type = "DOCUMENT"
                placeholder = "[Файл]"
            elif att_type in ("video", "video_note"):
                msg_type = "VIDEO"
                placeholder = "[Видео]"
            elif att_type in ("audio_message", "voice"):
                msg_type = "VOICE"
                placeholder = "[Голосовое]"
            elif att_type == "sticker":
                msg_type = "SYSTEM"
                placeholder = "[Стикер]"
            else:
                msg_type = "TEXT"
                placeholder = "[Вложение]"

            attachment_items.append(
                {"type": msg_type, "file_info": file_info, "placeholder": placeholder}
            )

    if not attachment_items:
        # Если нет вложений, добавляем текстовое сообщение
        attachment_items.append(
            {
                "type": "TEXT",
                "file_info": None,
                "placeholder": message_text or "[Сообщение]",
            }
        )
    else:
        # Если есть и текст, и вложения, текст отправляем отдельным сообщением
        if message_text:
            attachment_items.insert(
                0,
                {
                    "type": "TEXT",
                    "file_info": None,
                    "placeholder": message_text,
                },
            )

    first_message_id = None
    for idx, item in enumerate(attachment_items):
        item_type = item["type"]
        item_content = item["placeholder"]
        item_file_info = item["file_info"]

        if idx == 0 and external_message_id:
            existing_msg = await database.fetch_one(
                chat_messages.select().where(
                    (chat_messages.c.external_message_id == external_message_id)
                    & (chat_messages.c.chat_id == chat_db_id)
                )
            )
            if existing_msg:
                first_message_id = existing_msg["id"]
                print(f"[Max] Duplicate message, existing id={first_message_id}")
                continue

        if idx == 0:
            db_message = await crud.create_message_and_update_chat(
                chat_id=chat_db_id,
                sender_type=sender_type,
                content=item_content,
                message_type=item_type,
                external_message_id=external_message_id,
                status="DELIVERED",
                created_at=created_at,
                source="max",
            )
            msg_id = db_message["id"]
        else:
            values = {
                "chat_id": chat_db_id,
                "sender_type": sender_type,
                "content": item_content,
                "message_type": item_type,
                "external_message_id": external_message_id,
                "status": "DELIVERED",
                "source": "max",
                "created_at": created_at or datetime.utcnow(),
            }
            msg_id = await database.execute(chat_messages.insert().values(**values))
            await database.execute(
                chats.update()
                .where(chats.c.id == chat_db_id)
                .values(
                    last_message_time=datetime.utcnow(), updated_at=datetime.utcnow()
                )
            )

        if first_message_id is None:
            first_message_id = msg_id

        print(
            f"[Max] Saved message id={msg_id}, content='{item_content[:100]}', type={item_type}"
        )

        if item_file_info and item_type != "SYSTEM":
            await _process_attachment(
                file_info=item_file_info,
                attachment_type=item_type,
                message_id=msg_id,
                cashbox_id=cashbox_id,
                channel_id=channel_id,
                bot_token=bot_token,
            )

        try:
            await chat_producer.send_message(
                chat_db_id,
                {
                    "message_id": msg_id,
                    "chat_id": chat_db_id,
                    "channel_type": "MAX",
                    "external_message_id": external_message_id,
                    "sender_type": sender_type,
                    "content": item_content,
                    "message_type": item_type,
                    "created_at": datetime.utcnow().isoformat(),
                },
            )
        except Exception as exc:
            print(f"[Max] WARNING: RabbitMQ publish failed: {exc}")

        try:
            from api.chats.websocket import cashbox_manager, chat_manager

            ws_msg = {
                "type": "message",
                "chat_id": chat_db_id,
                "message_id": msg_id,
                "sender_type": sender_type,
                "content": item_content,
                "message_type": item_type,
                "status": "DELIVERED",
                "timestamp": datetime.utcnow().isoformat(),
            }
            await chat_manager.broadcast_to_chat(chat_db_id, ws_msg)
            await cashbox_manager.broadcast_to_cashbox(
                cashbox_id, {"type": "chat_message", "event": "new_message", **ws_msg}
            )
        except Exception as exc:
            print(f"[Max] WARNING: WebSocket broadcast failed: {exc}")

    # Автоответ (auto-reply) для клиентских сообщений, если нет contact-вложения
    if sender_type == "CLIENT" and not contact_attachments:
        # Получаем свежие метаданные чата
        fresh_chat = await crud.get_chat(chat_db_id)
        meta = fresh_chat.get("metadata") or {}
        if isinstance(meta, str):
            meta = json.loads(meta)
        contact = await get_contact_by_id(contact_id)
        phone = contact.get("phone") if contact else None
        print(
            f"[Max] Calling handle_incoming_message (auto-reply): phone={phone}, contact_id={contact_id}"
        )
        from api.chats.max.max_auto_reply import handle_incoming_message

        await handle_incoming_message(
            bot_token=bot_token,
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            max_chat_id=chat_id_external,
            max_user_id=sender_user_id,
            chat_id=chat_db_id,
            chat_metadata=meta,
            phone=phone,
            contact_id=contact_id,
        )
        await database.execute(
            chats.update()
            .where(chats.c.id == chat_db_id)
            .values(metadata=meta, updated_at=datetime.utcnow())
        )

    print("[Max] ========== END _handle_message ==========")
    return {
        "success": True,
        "message": "Message(s) processed",
        "chat_id": chat_db_id,
        "message_id": first_message_id,
    }


async def _process_attachment(
    file_info: Dict[str, Any],
    attachment_type: str,
    message_id: int,
    cashbox_id: int,
    channel_id: int,
    bot_token: str,
) -> None:
    try:
        direct_url = None
        token = file_info.get("token")

        if attachment_type == "VIDEO" and token:
            direct_url = f"max_video_token:{token}"
        else:
            # Для изображений и прочего – старая логика
            direct_url = file_info.get("url")
            if not direct_url:
                photos = file_info.get("photos")
                if photos and isinstance(photos, dict):
                    for size_key in sorted(
                        photos.keys(), key=lambda x: int(x.split("x")[0]), reverse=True
                    ):
                        entry = photos.get(size_key)
                        if entry:
                            direct_url = (
                                entry.get("url") if isinstance(entry, dict) else entry
                            )
                            if direct_url:
                                break

        if direct_url:
            await database.execute(
                chat_messages.update()
                .where(chat_messages.c.id == message_id)
                .values(content=direct_url, updated_at=datetime.utcnow())
            )
        else:
            print(f"[Max] WARNING: Could not resolve video URL for token {token}")
    except Exception as exc:
        print(f"[Max] ERROR in _process_attachment: {exc}")


async def _handle_bot_started(
    update: Dict[str, Any],
    channel_id: int,
    cashbox_id: int,
    bot_token: str,
) -> Dict[str, Any]:
    user = update.get("user") or {}
    chat_id_raw = update.get("chat_id") or update.get("user_id") or user.get("user_id")
    if not chat_id_raw:
        return {"success": True, "message": "bot_started: no chat_id"}

    chat_id_external = str(chat_id_raw)
    sender_user_id = user.get("user_id", 0)
    sender_name = _build_contact_name(user) or f"Max User {sender_user_id}"
    avatar_url_raw = user.get("avatar_url") or user.get("full_avatar_url")

    avatar_url = None
    if sender_user_id and avatar_url_raw:
        avatar_url = await _get_or_upload_avatar(
            avatar_url_raw, sender_user_id, cashbox_id, channel_id
        )

    existing = await crud.get_chat_by_external_id(
        channel_id=channel_id,
        external_chat_id=chat_id_external,
        cashbox_id=cashbox_id,
    )

    # Извлекаем payload и visit_id
    payload = update.get("payload")
    metadata = {
        "source": "max",
        "username": user.get("username"),
        "max_user_id": sender_user_id,
    }
    if payload and payload.startswith("visit_"):
        try:
            visit_id = int(payload.split("_")[1])
            metadata["visit_id"] = visit_id
            print(f"[Max] Extracted visit_id={visit_id} from bot_started payload")
        except (ValueError, IndexError):
            pass
    qr_page_id = None
    from_qr = False
    if metadata and metadata.get("visit_id"):
        visit = await database.fetch_one(
            qr_visits.select().where(qr_visits.c.id == metadata["visit_id"])
        )
        if visit:
            qr_page_id = visit["page_id"]
            from_qr = True
    if existing:
        chat = existing
        # --- ОБНОВЛЕНИЕ МЕТАДАННЫХ СУЩЕСТВУЮЩЕГО ЧАТА ---
        existing_meta = chat.get("metadata") or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except Exception:
                existing_meta = {}
        # Сброс флагов автоответа, если пришли с visit_id
        if metadata.get("visit_id"):
            keys_to_reset = [
                k
                for k in existing_meta
                if k.startswith("auto_reply_sent_")
                or k.startswith("auto_reply_msg_id_")
            ]
            for k in keys_to_reset:
                del existing_meta[k]
            print(f"[Max] Reset auto_reply flags for chat {chat['id']}")
        # Объединяем новые метаданные
        updated_meta = {**existing_meta, **metadata}
        if updated_meta != existing_meta:
            await database.execute(
                chats.update()
                .where(chats.c.id == chat["id"])
                .values(metadata=updated_meta, updated_at=datetime.utcnow())
            )
            chat["metadata"] = updated_meta
            print(
                f"[Max] Updated chat metadata with visit_id={metadata.get('visit_id')}"
            )

        # Обновляем аватар, если нужно
        if avatar_url:
            contacts = await get_chat_contacts(chat["id"])
            if contacts:
                contact_id = contacts[0]["id"]
                await database.execute(
                    chat_contacts.update()
                    .where(chat_contacts.c.id == contact_id)
                    .values(avatar=avatar_url, updated_at=datetime.utcnow())
                )
    else:
        chat = await crud.create_chat(
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            external_chat_id=chat_id_external,
            external_chat_id_for_contact=(
                str(sender_user_id) if sender_user_id else None
            ),
            name=sender_name,
            metadata=metadata,
            from_qr=from_qr,
            qr_page_id=qr_page_id,
        )

    if not chat:
        return {"success": True, "message": "bot_started: failed to create chat"}

    chat_db_id = chat["id"]

    last_activity = user.get("last_activity_time")
    contact_id = await get_or_create_contact(
        cashbox_id=cashbox_id,
        external_contact_id=str(sender_user_id),
        name=sender_name,
        avatar=avatar_url,
        phone=None,
        last_activity=last_activity,
    )
    await link_contact_to_chat(chat_db_id, contact_id)

    existing_link = await database.fetch_one(
        chat_contact_links.select().where(
            (chat_contact_links.c.chat_id == chat_db_id)
            & (chat_contact_links.c.contact_id == contact_id)
        )
    )
    if not existing_link:
        await database.execute(
            chat_contact_links.insert().values(
                chat_id=chat_db_id,
                contact_id=contact_id,
                role="participant",
                created_at=datetime.utcnow(),
            )
        )

    from api.chats.max.max_auto_reply import handle_incoming_message

    meta = chat.get("metadata") or {}
    if isinstance(meta, str):
        meta = json.loads(meta)
    contact = await get_contact_by_id(contact_id)
    phone = contact.get("phone") if contact else None
    print(
        f"[Max] bot_started: sending auto-reply, phone={phone}, contact_id={contact_id}"
    )
    await handle_incoming_message(
        bot_token=bot_token,
        channel_id=channel_id,
        cashbox_id=cashbox_id,
        max_chat_id=chat_id_external,
        max_user_id=sender_user_id,
        chat_id=chat_db_id,
        chat_metadata=meta,
        phone=phone,
        contact_id=contact_id,
    )
    await database.execute(
        chats.update()
        .where(chats.c.id == chat_db_id)
        .values(metadata=meta, updated_at=datetime.utcnow())
    )

    return {"success": True, "chat_id": chat_db_id}


async def send_operator_message(
    chat: Dict[str, Any],
    text: Optional[str],
    image_url: Optional[str],
    cashbox_id: int,
    bot_token: str,
    files: Optional[List[str]] = None,
    message_type: str = "TEXT",
) -> Optional[str]:
    """
    Отправляет сообщение от оператора.
    Если есть и текст, и изображение - отправляет двумя сообщениями.
    """
    client = MaxClient(bot_token)
    external_chat_id = chat.get("external_chat_id", "")
    metadata = chat.get("metadata") or {}
    max_user_id: Optional[int] = metadata.get("max_user_id")
    chat_type: str = metadata.get("chat_type", "dialog")

    send_kwargs: Dict[str, Any] = {}
    if chat_type == "dialog" and max_user_id:
        send_kwargs["user_id"] = max_user_id
    else:
        send_kwargs["chat_id"] = external_chat_id

    sent_ids = []

    # Отправляем изображение, если оно есть
    if message_type == "IMAGE" and (image_url or (files and files[0])):
        source_url = image_url or files[0]
        if source_url:
            file_bytes = await _prepare_file_bytes(source_url)
            if file_bytes:
                token = await client.upload_file(file_bytes, file_type="image")
                if token:
                    attachments = [{"type": "image", "payload": {"token": token}}]
                    result = await client.send_message(
                        text="", attachments=attachments, **send_kwargs
                    )
                    sent_ids.append(
                        result.get("message", {}).get("body", {}).get("mid")
                    )

    # Отправляем текст, если он есть
    if text:
        result = await client.send_message(text=text, **send_kwargs)
        sent_ids.append(result.get("message", {}).get("body", {}).get("mid"))

    # Обработка документов, видео, голосовых (если нужно)
    if message_type in ("DOCUMENT", "VIDEO", "VOICE") and files:
        file_type_map = {"DOCUMENT": "document", "VIDEO": "video", "VOICE": "audio"}
        max_type = file_type_map.get(message_type, "document")
        for file_url in files:
            file_bytes = await _prepare_file_bytes(file_url)
            if file_bytes:
                token = await client.upload_file(file_bytes, file_type=max_type)
                if token:
                    attachments = [{"type": max_type, "payload": {"token": token}}]
                    result = await client.send_message(
                        text=text or "", attachments=attachments, **send_kwargs
                    )
                    sent_ids.append(
                        result.get("message", {}).get("body", {}).get("mid")
                    )

    return sent_ids[0] if sent_ids else None
