# api/chats/telegram/telegram_handler.py

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from api.chats import crud
from api.chats.avito.avito_factory import _decrypt_credential
from api.chats.max.max_auto_reply import (
    _save_system_message,
)
from api.chats.producer import chat_producer
from api.chats.schemas import TELEGRAM_FILE_ID_PREFIX
from api.chats.telegram.telegram_client import (
    answer_callback_query,
    get_file,
    get_user_profile_photos,
    send_message,
)
from api.chats.websocket import cashbox_manager  # FIX: прямой broadcast без RabbitMQ
from api.qr.routes import qr_visits
from database.db import (
    channel_credentials,
    chat_contact_links,
    chat_messages,
    chats,
    database,
    files,
    pictures,
    relation_message_files,
)
from functions.helpers import clear_phone_number, get_any_cashbox_user_id
from segments.actions.segment_auto_reply import (
    CALLBACK_PREFIX as AUTO_REPLY_PREFIX,
    handle_auto_reply_callback,
    handle_contact_received as auto_reply_contact,
    handle_incoming_message as auto_reply_message,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_AVATAR_CACHE: Dict[str, str] = {}
_CASHBOX_USER_CACHE: Dict[int, int] = {}


def _build_contact_name(user: Dict[str, Any]) -> Optional[str]:
    first = user.get("first_name") or ""
    last = user.get("last_name") or ""
    username = user.get("username")
    full = f"{first} {last}".strip()
    if full:
        return full
    if username:
        return username
    return None


def _build_chat_name(
    user: Dict[str, Any],
    chat_external_id: str,
    chat_title: Optional[str] = None,
    contact_name: Optional[str] = None,
) -> str:
    if chat_title:
        return chat_title
    if contact_name:
        return contact_name
    username = user.get("username")
    if username:
        return username
    short_id = chat_external_id[:8] if chat_external_id else "unknown"
    return f"Telegram Chat {short_id}"


async def _get_cashbox_tg_relation_id(cashbox_id: int) -> Optional[int]:
    cached = _CASHBOX_USER_CACHE.get(cashbox_id)
    if cached is not None:
        return cached
    user_id = await get_any_cashbox_user_id(cashbox_id=cashbox_id)
    _CASHBOX_USER_CACHE[cashbox_id] = user_id
    return user_id


def _guess_extension(filename: Optional[str], mime_type: Optional[str]) -> str:
    if filename and "." in filename:
        return filename.rsplit(".", 1)[-1].lower()
    if mime_type:
        if "jpeg" in mime_type:
            return "jpg"
        if "png" in mime_type:
            return "png"
        if "gif" in mime_type:
            return "gif"
        if "pdf" in mime_type:
            return "pdf"
        if "mp4" in mime_type:
            return "mp4"
        if "ogg" in mime_type or "opus" in mime_type:
            return "ogg"
    return "bin"


async def _get_avatar_url(
    user_id: Optional[int],
    bot_token: str,
    cashbox_id: int,
    channel_id: int,
) -> Optional[str]:
    print(f"[Telegram] _get_avatar_url called for user_id={user_id}")
    if not user_id:
        return None
    cache_key = f"{channel_id}:{user_id}"
    if cache_key in _AVATAR_CACHE:
        return _AVATAR_CACHE[cache_key]
    try:
        photos = await get_user_profile_photos(bot_token, user_id, limit=1)
        photo_list = photos.get("photos") or []
        if not photo_list:
            print("[Telegram] No profile photos found")
            return None
        sizes = photo_list[0]
        best = max(sizes, key=lambda s: s.get("file_size") or s.get("width", 0))
        file_id = best.get("file_id")
        if not file_id:
            return None
        file_meta = await get_file(bot_token, file_id)
        file_path = file_meta.get("file_path")
        if not file_path:
            return None
        direct_url = f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
        _AVATAR_CACHE[cache_key] = direct_url
        print(f"[Telegram] Fetched avatar URL: {direct_url}")
        return direct_url
    except Exception as e:
        logger.warning(f"Failed to fetch avatar for user {user_id}: {e}")
        return None


async def _store_picture(
    message_id: int,
    file_url: str,
    cashbox_id: int,
    size: int = 0,
) -> Optional[int]:
    try:
        owner_id = await _get_cashbox_tg_relation_id(cashbox_id)
        pic_id = await database.execute(
            pictures.insert().values(
                entity="messages",
                entity_id=message_id,
                url=file_url,
                cashbox=cashbox_id,
                size=size,
                is_main=False,
                is_deleted=False,
                owner=owner_id,
            )
        )
        return pic_id
    except Exception as e:
        logger.error(f"Failed to store picture: {e}")
        return None


async def _store_file(
    mime_type: Optional[str],
    extension: str,
    file_name: Optional[str],
    file_url: str,
    cashbox_id: int,
    message_id: int,
    size: int = 0,
) -> Optional[int]:
    try:
        owner_id = await _get_cashbox_tg_relation_id(cashbox_id)
        file_id = await database.execute(
            files.insert().values(
                mime_type=mime_type or "application/octet-stream",
                extension=extension,
                title=file_name or f"file.{extension}",
                url=file_url,
                cashbox=cashbox_id,
                size=size,
                is_deleted=False,
                owner=owner_id,
            )
        )
        await database.execute(
            relation_message_files.insert().values(
                message_id=message_id,
                file_id=file_id,
            )
        )
        return file_id
    except Exception as e:
        logger.error(f"Failed to store file: {e}")
        return None


async def _ensure_chat(
    channel_id: int,
    cashbox_id: int,
    external_chat_id: str,
    external_contact_id: Optional[str],
    name: Optional[str],
    chat_name: Optional[str],
    metadata: Optional[Dict[str, Any]],
    avatar: Optional[str] = None,
) -> Dict[str, Any]:
    chat = await crud.get_chat_by_external_id(
        channel_id=channel_id, external_chat_id=external_chat_id, cashbox_id=cashbox_id
    )

    qr_page_id = None
    from_qr = False
    if metadata and metadata.get("visit_id"):
        visit = await database.fetch_one(
            qr_visits.select().where(qr_visits.c.id == metadata["visit_id"])
        )
        if visit:
            qr_page_id = visit["page_id"]
            from_qr = True
    if chat:
        # --- ОБНОВЛЕНИЕ МЕТАДАННЫХ ---
        if metadata:
            existing_meta = chat.get("metadata") or {}
            if isinstance(existing_meta, str):
                try:
                    existing_meta = json.loads(existing_meta)
                except Exception:
                    existing_meta = {}

            # Объединяем: новые поля перезаписывают старые
            updated_meta = {**existing_meta, **metadata}
            if updated_meta != existing_meta:
                await database.execute(
                    chats.update()
                    .where(chats.c.id == chat["id"])
                    .values(metadata=updated_meta, updated_at=datetime.utcnow())
                )
                chat["metadata"] = updated_meta

        # Если в метаданных появился новый visit_id, сбрасываем флаги автоответа
        if metadata and metadata.get("visit_id"):
            # Сбросить все ключи auto_reply_*
            if chat.get("metadata"):
                meta = chat["metadata"]
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        meta = {}
                keys_to_reset = [
                    k
                    for k in meta
                    if k.startswith("auto_reply_sent_")
                    or k.startswith("auto_reply_msg_id_")
                ]
                if keys_to_reset:
                    for k in keys_to_reset:
                        del meta[k]
                    await database.execute(
                        chats.update()
                        .where(chats.c.id == chat["id"])
                        .values(metadata=meta, updated_at=datetime.utcnow())
                    )
                    chat["metadata"] = meta

        if from_qr and not chat.get("from_qr"):
            await database.execute(
                chats.update()
                .where(chats.c.id == chat["id"])
                .values(
                    from_qr=True, qr_page_id=qr_page_id, updated_at=datetime.utcnow()
                )
            )
        if avatar and (not chat.get("contact") or not chat["contact"].get("avatar")):
            contact_id = await crud.get_or_create_chat_contact(
                cashbox_id=cashbox_id,
                external_contact_id=external_contact_id,
                name=name,
                avatar=avatar,
                allow_name_fallback=False,
            )
            # Связать контакт с чатом, если ещё не связан
            existing_link = await database.fetch_one(
                chat_contact_links.select().where(
                    (chat_contact_links.c.chat_id == chat["id"])
                    & (chat_contact_links.c.contact_id == contact_id)
                )
            )
            if not existing_link:
                await database.execute(
                    chat_contact_links.insert().values(
                        chat_id=chat["id"],
                        contact_id=contact_id,
                        role="participant",
                        created_at=datetime.utcnow(),
                    )
                )

        # Обновление ad_title при необходимости
        if chat_name:
            existing_meta = chat.get("metadata") or {}
            if isinstance(existing_meta, str):
                try:
                    existing_meta = json.loads(existing_meta)
                except Exception:
                    existing_meta = {}
            if not existing_meta.get("ad_title"):
                existing_meta["ad_title"] = chat_name
                await database.execute(
                    chats.update()
                    .where(chats.c.id == chat["id"])
                    .values(metadata=existing_meta, updated_at=datetime.utcnow())
                )

        return chat

    # --- СОЗДАНИЕ НОВОГО ЧАТА ---
    if chat_name:
        metadata = dict(metadata or {})
        metadata.setdefault("ad_title", chat_name)

    contact_id = await crud.get_or_create_chat_contact(
        cashbox_id=cashbox_id,
        external_contact_id=external_contact_id,
        name=name,
        avatar=avatar,
        allow_name_fallback=False,
    )

    chat = await crud.create_chat(
        channel_id=channel_id,
        cashbox_id=cashbox_id,
        external_chat_id=external_chat_id,
        chat_contact_id=contact_id,
        name=chat_name or name,
        metadata=metadata,
        from_qr=from_qr,
        qr_page_id=qr_page_id,
    )
    return chat


def _get_chat_phone(chat: Dict[str, Any]) -> Optional[str]:
    contact = chat.get("contact") or {}
    return contact.get("phone") or None


def _get_chat_metadata(chat: Dict[str, Any]) -> Dict:
    meta = chat.get("metadata") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    return dict(meta) if isinstance(meta, dict) else {}


async def _save_chat_metadata(chat_id: int, meta: Dict) -> None:
    await database.execute(
        chats.update()
        .where(chats.c.id == chat_id)
        .values(metadata=meta, updated_at=datetime.utcnow())
    )


async def handle_update(
    update: Dict[str, Any],
    channel_id: int,
    cashbox_id: int,
    bot_token: str,
) -> Dict[str, Any]:
    print("[Telegram] ========== HANDLE UPDATE ==========")
    print(
        f"[Telegram] update_id={update.get('update_id')}, channel={channel_id}, cashbox={cashbox_id}"
    )

    callback_query = update.get("callback_query")
    if callback_query:
        print("[Telegram] Processing callback_query")
        message = callback_query.get("message") or {}
        chat_info = message.get("chat") or {}
        user_info = callback_query.get("from") or {}
        chat_external_id = str(chat_info.get("id") or "")

        if not chat_external_id:
            return {"success": True, "message": "No chat id"}

        external_contact_id = (
            str(user_info.get("id")) if user_info.get("id") else chat_external_id
        )

        avatar_url = await _get_avatar_url(
            user_info.get("id"),
            bot_token,
            cashbox_id=cashbox_id,
            channel_id=channel_id,
        )

        contact_name = _build_contact_name(user_info)
        chat_name = _build_chat_name(
            user_info,
            chat_external_id,
            chat_title=chat_info.get("title"),
            contact_name=contact_name,
        )
        chat = await _ensure_chat(
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            external_chat_id=chat_external_id,
            external_contact_id=external_contact_id,
            name=contact_name,
            chat_name=chat_name,
            metadata={"source": "telegram", "username": user_info.get("username")},
            avatar=avatar_url,
        )

        cb_data: str = callback_query.get("data") or ""
        if cb_data.startswith(AUTO_REPLY_PREFIX + ":"):
            phone = _get_chat_phone(chat)
            handled = await handle_auto_reply_callback(
                bot_token=bot_token,
                callback_query=callback_query,
                channel_id=channel_id,
                cashbox_id=cashbox_id,
                chat_id=chat["id"],
                telegram_chat_id=chat_external_id,
                phone=phone,
            )
            if handled:
                return {"success": True, "message": "AutoReply callback processed"}

        content = callback_query.get("data") or "[Button]"
        existing = await database.fetch_one(
            chat_messages.select().where(
                (chat_messages.c.external_message_id == callback_query.get("id"))
                & (chat_messages.c.chat_id == chat["id"])
            )
        )
        if not existing:
            message_db = await crud.create_message_and_update_chat(
                chat_id=chat["id"],
                sender_type="CLIENT",
                content=content,
                message_type="SYSTEM",
                external_message_id=callback_query.get("id"),
                status="DELIVERED",
                source="telegram",
            )
            await chat_producer.send_message(
                chat_id=chat["id"],
                message_data={
                    "message_id": message_db["id"],
                    "chat_id": chat["id"],
                    "channel_type": "TELEGRAM",
                    "external_message_id": callback_query.get("id"),
                    "sender_type": "CLIENT",
                    "content": content,
                    "message_type": "SYSTEM",
                    "created_at": datetime.utcnow().isoformat(),
                },
            )

        try:
            await answer_callback_query(bot_token, callback_query.get("id"), text="OK")
        except Exception:
            pass

        return {"success": True, "message": "Callback processed"}

    message = update.get("message") or update.get("edited_message")
    if not message:
        return {"success": True, "message": "No supported update"}

    chat_info = message.get("chat") or {}
    user_info = message.get("from") or {}
    chat_external_id = str(chat_info.get("id") or "")
    if not chat_external_id:
        return {"success": True, "message": "No chat id"}

    # Базовые метаданные
    metadata = {
        "source": "telegram",
        "username": user_info.get("username"),
        "chat_title": chat_info.get("title"),
        "chat_type": chat_info.get("type"),
    }

    # Извлекаем visit_id ДО вызова _ensure_chat
    if message.get("text", "").startswith("/start"):
        parts = message["text"].split()
        if len(parts) > 1 and parts[1].startswith("visit_"):
            try:
                visit_id = int(parts[1].split("_")[1])
                metadata["visit_id"] = visit_id
                print(f"[Telegram] Extracted visit_id={visit_id} from /start command")
            except (ValueError, IndexError) as e:
                print(f"[Telegram] Failed to parse visit_id: {e}")

    external_contact_id = (
        str(user_info.get("id")) if user_info.get("id") else chat_external_id
    )

    avatar_url = await _get_avatar_url(
        user_info.get("id"),
        bot_token,
        cashbox_id=cashbox_id,
        channel_id=channel_id,
    )

    contact_name = _build_contact_name(user_info)
    chat_name = _build_chat_name(
        user_info,
        chat_external_id,
        chat_title=chat_info.get("title"),
        contact_name=contact_name,
    )

    # Вызываем _ensure_chat с уже обогащёнными метаданными
    chat = await _ensure_chat(
        channel_id=channel_id,
        cashbox_id=cashbox_id,
        external_chat_id=chat_external_id,
        external_contact_id=external_contact_id,
        name=contact_name,
        chat_name=chat_name,
        metadata=metadata,
        avatar=avatar_url,
    )
    print(f"[Telegram] Chat ensured, id={chat['id']}, metadata={chat.get('metadata')}")

    sender_type = "OPERATOR" if user_info.get("is_bot") else "CLIENT"

    if message.get("text", "").startswith("/start"):
        parts = message["text"].split()
        if len(parts) > 1 and parts[1].startswith("visit_"):
            try:
                visit_id = int(parts[1].split("_")[1])
                metadata["visit_id"] = visit_id
            except (ValueError, IndexError):
                pass
    if message.get("contact") and sender_type == "CLIENT":
        contact = message["contact"]
        raw_phone = contact.get("phone_number")
        first_name = contact.get("first_name")
        last_name = contact.get("last_name")
        if raw_phone:
            phone = clear_phone_number(raw_phone)
            print(f"[Telegram] Contact received: raw={raw_phone}, cleaned={phone}")
        else:
            phone = None

        if phone:
            try:
                result = await crud.chain_client(
                    chat_id=chat["id"],
                    phone=phone,
                    name=f"{first_name or ''} {last_name or ''}".strip() or None,
                )
                print(f"[Telegram] chain_client result: {result}")
            except Exception as e:
                print(f"[Telegram] chain_client FAILED: {e}")
                await send_message(
                    token=bot_token,
                    chat_id=chat_external_id,
                    text="Произошла ошибка при регистрации. Попробуйте позже.",
                )
                await _save_system_message(
                    chat["id"], f"Ошибка регистрации: {e}", cashbox_id
                )
                return {"success": True, "message": "Contact processed with error"}

            await crud.update_chat(
                chat["id"],
                phone=phone,
                name=contact.get("first_name") or contact.get("last_name"),
            )
            chat = await crud.get_chat(chat["id"])
            meta = _get_chat_metadata(chat)
            reset = {k: False for k in meta if k.startswith("auto_reply_sent_")}
            if reset:
                meta.update(reset)
                await _save_chat_metadata(chat["id"], meta)

            await auto_reply_contact(
                bot_token=bot_token,
                cashbox_id=cashbox_id,
                telegram_chat_id=chat_external_id,
                phone=phone,
                first_name=contact.get("first_name"),
                last_name=contact.get("last_name"),
                chat_id=chat["id"],
            )

        return {"success": True, "message": "Contact processed"}

    # Собираем вложения
    attachments = []
    if message.get("photo"):
        photo_sizes = message["photo"]
        best_photo = max(
            photo_sizes, key=lambda item: item.get("file_size") or item.get("width", 0)
        )
        attachments.append(("IMAGE", best_photo))
    if message.get("document"):
        attachments.append(("DOCUMENT", message["document"]))
    if message.get("video"):
        attachments.append(("VIDEO", message["video"]))
    if message.get("voice"):
        attachments.append(("VOICE", message["voice"]))

    message_text = message.get("text") or message.get("caption")
    external_message_id = str(message.get("message_id") or "")

    if not attachments and not message_text:
        return {"success": True, "message": "Empty message ignored"}

    created_at = None
    if message.get("date"):
        try:
            created_at = datetime.utcfromtimestamp(message["date"])
        except Exception:
            created_at = None

    # Текстовое сообщение (если есть текст)
    if message_text:
        text_msg_db = await crud.create_message_and_update_chat(
            chat_id=chat["id"],
            sender_type=sender_type,
            content=message_text,
            message_type="TEXT",
            external_message_id=external_message_id,
            status="DELIVERED",
            created_at=created_at,
            source="telegram",
        )
        print(
            f"[Telegram] Saved text message {text_msg_db['id']} content='{message_text[:50]}'"
        )
        await chat_producer.send_message(
            chat_id=chat["id"],
            message_data={
                "message_id": text_msg_db["id"],
                "chat_id": chat["id"],
                "channel_type": "TELEGRAM",
                "external_message_id": external_message_id,
                "sender_type": sender_type,
                "content": message_text,
                "message_type": "TEXT",
                "created_at": datetime.utcnow().isoformat(),
            },
        )
        # FIX: прямой broadcast для мгновенного обновления UserList
        try:
            _ts = text_msg_db.get("created_at")
            _ts_str = (
                _ts.isoformat()
                if hasattr(_ts, "isoformat")
                else datetime.utcnow().isoformat()
            )
            await cashbox_manager.broadcast_to_cashbox(
                cashbox_id,
                {
                    "type": "chat_message",
                    "event": "new_message",
                    "chat_id": chat["id"],
                    "message_id": text_msg_db["id"],
                    "sender_type": sender_type,
                    "content": message_text,
                    "message_type": "TEXT",
                    "timestamp": _ts_str,
                },
            )
        except Exception as _e:
            logger.debug("cashbox direct broadcast failed: %s", _e)

    # Вложения – каждое отдельным сообщением
    for idx, (att_type, file_info) in enumerate(attachments):
        unique_ext_id = f"{external_message_id}_{idx}" if external_message_id else None
        placeholder = {
            "IMAGE": "[Photo]",
            "DOCUMENT": "[Document]",
            "VIDEO": "[Video]",
            "VOICE": "[Voice]",
        }.get(att_type, "[Media]")

        if unique_ext_id:
            existing = await database.fetch_one(
                chat_messages.select().where(
                    (chat_messages.c.external_message_id == unique_ext_id)
                    & (chat_messages.c.chat_id == chat["id"])
                )
            )
            if existing:
                continue

        media_msg_db = await crud.create_message_and_update_chat(
            chat_id=chat["id"],
            sender_type=sender_type,
            content=placeholder,
            message_type=att_type,
            external_message_id=unique_ext_id,
            status="DELIVERED",
            created_at=created_at,
            source="telegram",
        )
        print(f"[Telegram] Saved media message {media_msg_db['id']} type={att_type}")

        file_id = file_info.get("file_id")
        if file_id:
            try:
                file_meta = await get_file(bot_token, file_id)
                file_path = file_meta.get("file_path")
                if file_path:
                    direct_url = (
                        f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
                    )
                    await database.execute(
                        chat_messages.update()
                        .where(chat_messages.c.id == media_msg_db["id"])
                        .values(content=direct_url, updated_at=datetime.utcnow())
                    )
                    media_content = direct_url
                else:
                    media_content = placeholder
            except Exception as e:
                logger.warning(f"Failed to get file path for {file_id}: {e}")
                media_content = placeholder
        else:
            media_content = placeholder

        # Сохраняем в pictures/files
        if att_type == "IMAGE":
            await _store_picture(
                message_id=media_msg_db["id"],
                file_url=(
                    media_content
                    if media_content != placeholder
                    else f"{TELEGRAM_FILE_ID_PREFIX}{file_id}"
                ),
                cashbox_id=cashbox_id,
            )
        else:
            filename = file_info.get("file_name")
            mime_type = file_info.get("mime_type")
            extension = _guess_extension(filename, mime_type)
            await _store_file(
                mime_type=mime_type,
                extension=extension,
                file_name=filename,
                file_url=(
                    media_content
                    if media_content != placeholder
                    else f"{TELEGRAM_FILE_ID_PREFIX}{file_id}"
                ),
                cashbox_id=cashbox_id,
                message_id=media_msg_db["id"],
                size=file_info.get("file_size", 0),
            )

        await chat_producer.send_message(
            chat_id=chat["id"],
            message_data={
                "message_id": media_msg_db["id"],
                "chat_id": chat["id"],
                "channel_type": "TELEGRAM",
                "external_message_id": unique_ext_id,
                "sender_type": sender_type,
                "content": media_content,
                "message_type": att_type,
                "created_at": datetime.utcnow().isoformat(),
            },
        )
        # FIX: прямой broadcast для мгновенного обновления UserList
        try:
            _ts2 = media_msg_db.get("created_at")
            _ts2_str = (
                _ts2.isoformat()
                if hasattr(_ts2, "isoformat")
                else datetime.utcnow().isoformat()
            )
            await cashbox_manager.broadcast_to_cashbox(
                cashbox_id,
                {
                    "type": "chat_message",
                    "event": "new_message",
                    "chat_id": chat["id"],
                    "message_id": media_msg_db["id"],
                    "sender_type": sender_type,
                    "content": media_content,
                    "message_type": att_type,
                    "timestamp": _ts2_str,
                },
            )
        except Exception as _e2:
            logger.debug("cashbox direct broadcast (media) failed: %s", _e2)

    # Автоответ
    if sender_type == "CLIENT" and not message.get("contact"):
        fresh_chat = await crud.get_chat(chat["id"])
        chat_meta = _get_chat_metadata(fresh_chat)
        phone = _get_chat_phone(fresh_chat)

        await auto_reply_message(
            bot_token=bot_token,
            channel_id=channel_id,
            cashbox_id=cashbox_id,
            telegram_chat_id=chat_external_id,
            chat_id=chat["id"],
            chat_metadata=chat_meta,
            phone=phone,
        )
        await _save_chat_metadata(chat["id"], chat_meta)

    print("[Telegram] ========== END HANDLE UPDATE ==========")
    return {"success": True, "message": "Telegram update processed"}


async def refresh_telegram_avatar(
    channel_id: int,
    cashbox_id: int,
    external_contact_id: Optional[str],
) -> Optional[str]:
    if not external_contact_id:
        return None
    try:
        user_id = int(external_contact_id)
    except (TypeError, ValueError):
        return None

    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel_id)
            & (channel_credentials.c.cashbox_id == cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    if not creds or not creds.get("api_key"):
        return None

    try:
        bot_token = _decrypt_credential(creds["api_key"])
    except Exception:
        return None

    return await _get_avatar_url(
        user_id=user_id,
        bot_token=bot_token,
        cashbox_id=cashbox_id,
        channel_id=channel_id,
    )
