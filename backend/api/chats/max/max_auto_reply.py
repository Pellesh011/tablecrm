import json
import logging
from datetime import datetime, timedelta
from random import randint
from typing import Dict, List, Optional

from api.chats.contact_service import update_contact_phone
from api.chats.crud import mark_qr_registration
from database.db import contragents, database, loyality_cards, segments

logger = logging.getLogger(__name__)
DEBUG_PRINT = True
CALLBACK_PREFIX = "auto_reply"


async def _save_system_message(chat_id: int, text: str, cashbox_id: int = None) -> None:
    from database.db import chat_messages

    msg_id = await database.execute(
        chat_messages.insert().values(
            chat_id=chat_id,
            sender_type="OPERATOR",
            message_type="SYSTEM",
            content=text,
            status="DELIVERED",
            source="auto_reply",
            created_at=datetime.utcnow(),
        )
    )
    logger.info(f"[MaxAutoReply] System message saved: {text[:60]}")

    try:
        from api.chats.websocket import cashbox_manager, chat_manager

        ws_msg = {
            "type": "message",
            "chat_id": chat_id,
            "message_id": msg_id,
            "sender_type": "OPERATOR",
            "content": text,
            "message_type": "SYSTEM",
            "status": "DELIVERED",
            "timestamp": datetime.utcnow().isoformat(),
        }
        await chat_manager.broadcast_to_chat(chat_id, ws_msg)
        if cashbox_id:
            await cashbox_manager.broadcast_to_cashbox(
                cashbox_id,
                {"type": "chat_message", "event": "new_message", **ws_msg},
            )
    except Exception as e:
        logger.warning(f"[MaxAutoReply] WebSocket broadcast failed: {e}")


async def _get_auto_reply_segments(cashbox_id: int, channel_id: int) -> List[Dict]:
    print(
        f"[MaxAutoReply] _get_auto_reply_segments: cashbox_id={cashbox_id}, channel_id={channel_id}"
    )
    rows = await database.fetch_all(
        segments.select().where(
            segments.c.cashbox_id == cashbox_id,
            segments.c.is_deleted.isnot(True),
            segments.c.is_archived == False,
        )
    )
    print(f"[MaxAutoReply] Total segments in DB for cashbox: {len(rows)}")
    result = []
    for row in rows:
        raw = row.get("auto_reply")
        if not raw:
            continue
        ar = json.loads(raw) if isinstance(raw, str) else dict(raw)
        print(f"[MaxAutoReply] Segment id={row['id']}, auto_reply={ar}")
        if not ar.get("enabled"):
            print(f"[MaxAutoReply] Segment {row['id']} not enabled")
            continue
        if ar.get("channel_id") != channel_id:
            print(
                f"[MaxAutoReply] Segment {row['id']} channel_id mismatch: {ar.get('channel_id')} != {channel_id}"
            )
            continue
        result.append({"segment_id": row["id"], "config": ar})
    print(
        f"[MaxAutoReply] Found {len(result)} active auto-reply segments for channel {channel_id}"
    )
    return result


async def _find_contragent_by_phone(phone: Optional[str], cashbox_id: int):
    if not phone:
        return None
    return await database.fetch_one(
        contragents.select().where(
            contragents.c.phone == phone,
            contragents.c.cashbox == cashbox_id,
            contragents.c.is_deleted.isnot(True),
        )
    )


async def _has_loyalty_card(contragent_id: int, cashbox_id: int) -> bool:
    card = await database.fetch_one(
        loyality_cards.select().where(
            loyality_cards.c.contragent_id == contragent_id,
            loyality_cards.c.cashbox_id == cashbox_id,
            loyality_cards.c.is_deleted.isnot(True),
        )
    )
    return card is not None


async def _check_conditions(
    conditions: List[Dict], cashbox_id: int, phone: Optional[str]
) -> List[Dict]:
    print(f"[MaxAutoReply] _check_conditions: phone={phone}, conditions={conditions}")
    triggered = []
    contragent = await _find_contragent_by_phone(phone, cashbox_id)
    print(f"[MaxAutoReply] contragent found: {contragent is not None}")
    for cond in conditions:
        ctype = cond.get("type")
        if ctype == "not_registered":
            if not phone or contragent is None:
                triggered.append(cond)
                print("[MaxAutoReply] condition not_registered triggered")
        elif ctype == "no_loyalty_card":
            if contragent is not None and not await _has_loyalty_card(
                contragent.id, cashbox_id
            ):
                triggered.append(cond)
                print("[MaxAutoReply] condition no_loyalty_card triggered")
    return triggered


def _build_buttons(
    triggered: List[Dict], segment_id: int
) -> Optional[List[List[Dict]]]:
    print(
        f"[MaxAutoReply] _build_buttons: triggered={triggered}, segment_id={segment_id}"
    )
    rows = []
    for cond in triggered:
        ctype = cond.get("type")
        text = cond.get("button_text", "Действие")
        if ctype == "not_registered":
            rows.append(
                [
                    {
                        "type": "request_contact",
                        "text": text,
                    }
                ]
            )
            print(f"[MaxAutoReply] Added request_contact button: {text}")
        elif ctype == "no_loyalty_card":
            payload = f"{CALLBACK_PREFIX}:register_loyalty:{segment_id}"
            rows.append(
                [
                    {
                        "type": "callback",
                        "text": text,
                        "payload": payload,
                    }
                ]
            )
            print(f"[MaxAutoReply] Added callback button: {text} -> {payload}")
    print(f"[MaxAutoReply] Built buttons: {rows}")
    return rows if rows else None


async def handle_incoming_message(
    *,
    bot_token: str,
    channel_id: int,
    cashbox_id: int,
    max_chat_id: str,
    max_user_id: int,
    chat_id: int,
    chat_metadata: Dict,
    phone: Optional[str],
    contact_id: int,
) -> None:
    print(
        f"[MaxAutoReply] handle_incoming_message: chat_id={chat_id}, phone={phone}, channel_id={channel_id}, max_user_id={max_user_id}, contact_id={contact_id}"
    )
    print(f"[MaxAutoReply] chat_metadata={chat_metadata}")

    if chat_metadata.get("auto_reply_done"):
        print("[MaxAutoReply] auto_reply_done flag is set, skipping")
        return

    ar_segments = await _get_auto_reply_segments(cashbox_id, channel_id)
    if not ar_segments:
        print("[MaxAutoReply] No segments, exiting")
        return

    from api.chats.max.max_client import MaxClient

    client = MaxClient(bot_token)

    for item in ar_segments:
        segment_id = item["segment_id"]
        config = item["config"]
        sent_key = f"auto_reply_sent_{segment_id}"

        fresh_meta = await _get_chat_metadata(chat_id)
        if fresh_meta.get(sent_key):
            continue

        conditions = config.get("conditions", [])
        if not conditions:
            continue

        triggered = await _check_conditions(conditions, cashbox_id, phone)
        if not triggered:
            continue

        greeting = config.get("greeting_text") or "Добро пожаловать! Выберите действие:"
        buttons = _build_buttons(triggered, segment_id)
        if not buttons:
            continue

        result = await client.send_message_with_inline_keyboard(
            text=greeting,
            user_id=max_user_id,
            chat_id=max_chat_id,
            buttons=buttons,
        )

        chat_metadata[sent_key] = True
        sent_msg_id = result.get("message", {}).get("body", {}).get("mid")
        if sent_msg_id:
            chat_metadata[f"auto_reply_msg_id_{segment_id}"] = sent_msg_id

        from api.chats import crud

        await crud.update_chat(chat_id, metadata=chat_metadata)

        await _save_system_message(
            chat_id,
            f"Отправлены кнопки: {', '.join([c.get('button_text', '?') for c in triggered])}",
            cashbox_id,
        )
        break


async def _get_chat_metadata(chat_id: int) -> Dict:
    from api.chats import crud

    chat = await crud.get_chat(chat_id)
    meta = chat.get("metadata") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except:
            meta = {}
    return meta


async def handle_callback(
    *,
    bot_token: str,
    payload: str,
    channel_id: int,
    cashbox_id: int,
    chat_id: int,
    max_chat_id: str,
    max_user_id: int,
    phone: Optional[str],
    callback_id: str,
    contact_id: int,
) -> bool:
    if not payload.startswith(CALLBACK_PREFIX + ":"):
        return False

    parts = payload.split(":")
    if len(parts) < 3:
        return False

    action = parts[1]
    try:
        segment_id = int(parts[2])
    except ValueError:
        return False

    logger.info(
        f"[MaxAutoReply] Callback: action={action}, segment_id={segment_id}, chat_id={chat_id}"
    )

    # Получаем имя кнопки из конфигурации сегмента
    ar_segments = await _get_auto_reply_segments(cashbox_id, channel_id)
    button_name = action
    for item in ar_segments:
        if item["segment_id"] == segment_id:
            config = item["config"]
            for cond in config.get("conditions", []):
                if cond.get("type") == action:
                    button_name = cond.get("button_text", action)
                    break
            break

    from api.chats.max.max_client import MaxClient

    client = MaxClient(bot_token)

    from api.chats import crud

    chat = await crud.get_chat(chat_id)
    meta = chat.get("metadata") or {}
    if isinstance(meta, str):
        meta = json.loads(meta)
    button_msg_id = meta.get(f"auto_reply_msg_id_{segment_id}")

    if button_msg_id:
        try:
            await client.delete_message(button_msg_id)
            logger.info(f"[MaxAutoReply] Deleted button message {button_msg_id}")
        except Exception as e:
            logger.warning(f"Failed to delete message: {e}")

    success_text = None
    try:
        if action == "register_loyalty":
            result = await _action_register_loyalty(cashbox_id, phone, chat_id)
            if result == "basic_card_created":
                success_text = "успешно (базовая карта, нет автонастройки)"
                msg_to_user = (
                    "Вам выдана базовая карта лояльности (настройки не заданы)."
                )
            elif result == "already_exists":
                success_text = "уже существует"
                msg_to_user = "У вас уже есть карта лояльности."
            else:
                success_text = "успешно"
                msg_to_user = "Карта лояльности успешно зарегистрирована! ✅"
            await client.send_message(
                text=msg_to_user,
                user_id=max_user_id,
                chat_id=max_chat_id,
            )
            await _save_system_message(
                chat_id,
                f"Пользователь нажал кнопку '{button_name}' - {success_text}",
                cashbox_id,
            )
        else:
            success_text = "успешно"
            await _save_system_message(
                chat_id,
                f"Пользователь нажал кнопку '{button_name}' - {success_text}",
                cashbox_id,
            )
    except Exception as e:
        logger.error(f"[MaxAutoReply] Action error: {e}")
        success_text = f"ошибка: {str(e)}"
        await _save_system_message(
            chat_id,
            f"Пользователь нажал кнопку '{button_name}' - {success_text}",
            cashbox_id,
        )
        try:
            await client.send_message(
                text="Произошла ошибка, попробуйте позже.",
                user_id=max_user_id,
                chat_id=max_chat_id,
            )
        except Exception:
            pass

    try:
        await client.answer_callback_query(callback_id, text=success_text or "Готово")
    except Exception as e:
        logger.warning(f"answer_callback_query failed: {e}")

    await _maybe_mark_done(cashbox_id, channel_id, chat_id, segment_id, phone)
    return True


async def handle_contact_received(
    *,
    bot_token: str,
    cashbox_id: int,
    max_chat_id: str,
    max_user_id: int,
    phone: str,
    first_name: Optional[str],
    last_name: Optional[str],
    chat_id: int,
    contact_id: int,
) -> None:
    from api.chats import crud
    from api.chats.crud import mark_qr_registration
    from api.chats.max.max_client import MaxClient

    print("[MaxAutoReply] ===== handle_contact_received START =====")
    print(
        f"[MaxAutoReply] phone={phone}, chat_id={chat_id}, contact_id={contact_id}, name={first_name} {last_name}"
    )

    try:
        print("[MaxAutoReply] Calling crud.chain_client...")
        result = await crud.chain_client(
            chat_id=chat_id,
            phone=phone,
            name=f"{first_name or ''} {last_name or ''}".strip() or None,
        )
        print(f"[MaxAutoReply] chain_client result: {result}")
        if result and result.get("contragent_id"):
            print(f"[MaxAutoReply] SUCCESS: contragent_id={result['contragent_id']}")
            # Обновляем телефон в контакте (на случай, если его не было)
            await update_contact_phone(contact_id, phone)
        else:
            print("[MaxAutoReply] WARNING: chain_client returned no contragent_id")
    except Exception as e:
        print(f"[MaxAutoReply] chain_client FAILED: {e}")
        logger.error(f"[MaxAutoReply] chain_client failed: {e}", exc_info=True)
        client = MaxClient(bot_token)
        await client.send_message(
            text="Произошла ошибка при регистрации. Попробуйте позже.",
            user_id=max_user_id,
            chat_id=max_chat_id,
        )
        await _save_system_message(chat_id, f"Ошибка регистрации: {e}", cashbox_id)
        return

    print("[MaxAutoReply] Sending confirmation to user...")
    client = MaxClient(bot_token)
    await client.send_message(
        text="Вы успешно зарегистрированы! ✅",
        user_id=max_user_id,
        chat_id=max_chat_id,
    )
    if result and result.get("contragent_id"):
        print(f"[MaxAutoReply] SUCCESS: contragent_id={result['contragent_id']}")
        await update_contact_phone(contact_id, phone)
        await mark_qr_registration(chat_id)
    else:
        print("[MaxAutoReply] WARNING: chain_client returned no contragent_id")
    await _save_system_message(
        chat_id, f"Пользователь зарегистрирован: {phone}", cashbox_id
    )
    print("[MaxAutoReply] ===== handle_contact_received END =====")


async def _action_register_loyalty(
    cashbox_id: int, phone: Optional[str], chat_id: int
) -> str:
    if not phone:
        raise ValueError("Нет телефона — невозможно создать карту лояльности")
    contragent = await _find_contragent_by_phone(phone, cashbox_id)
    if not contragent:
        raise ValueError(f"Контрагент с телефоном {phone} не найден")
    if await _has_loyalty_card(contragent.id, cashbox_id):
        logger.info(
            f"[MaxAutoReply] Loyalty card already exists for contragent {contragent.id}"
        )
        return "already_exists"

    from database.db import loyality_settings, organizations
    from functions.helpers import clear_phone_number

    settings = await database.fetch_one(
        loyality_settings.select().where(loyality_settings.c.cashbox == cashbox_id)
    )

    default_org = await database.fetch_one(
        organizations.select().where(
            organizations.c.cashbox == cashbox_id,
            organizations.c.is_deleted == False,
        )
    )
    org_id = default_org.id if default_org else None

    card_number = (
        clear_phone_number(phone) if phone else randint(0, 9_223_372_036_854_775)
    )

    base_values = {
        "contragent_id": contragent.id,
        "cashbox_id": cashbox_id,
        "card_number": card_number,
        "balance": 0,
        "income": 0,
        "outcome": 0,
        "is_deleted": False,
        "status_card": True,
        "apple_wallet_advertisement": "",
        "organization_id": org_id,
    }

    if settings:
        base_values.update(
            {
                "cashback_percent": settings.cashback_percent or 0,
                "minimal_checque_amount": settings.minimal_checque_amount or 0,
                "max_percentage": settings.max_percentage or 0,
                "max_withdraw_percentage": settings.max_withdraw_percentage or 0,
                "start_period": settings.start_period,
                "end_period": settings.end_period,
                "lifetime": settings.lifetime,
                "tags": settings.tags,
            }
        )
        result_type = "card_created"
    else:
        base_values.update(
            {
                "cashback_percent": 0,
                "minimal_checque_amount": 0,
                "max_percentage": 0,
                "max_withdraw_percentage": 0,
                "start_period": datetime.now(),
                "end_period": datetime.now() + timedelta(days=365 * 10),
                "lifetime": 0,
                "apple_wallet_advertisement": "",
            }
        )
        result_type = "basic_card_created"
    await mark_qr_registration(chat_id)
    await database.execute(loyality_cards.insert().values(**base_values))
    logger.info(
        f"[MaxAutoReply] Created loyalty card for contragent {contragent.id} ({result_type})"
    )
    return result_type


async def _maybe_mark_done(
    cashbox_id: int,
    channel_id: int,
    chat_id: int,
    segment_id: int,
    phone: Optional[str],
) -> None:
    from api.chats import crud
    from database.db import chats

    ar_segments = await _get_auto_reply_segments(cashbox_id, channel_id)
    for item in ar_segments:
        if item["segment_id"] != segment_id:
            continue
        conditions = item["config"].get("conditions", [])
        triggered = await _check_conditions(conditions, cashbox_id, phone)
        if not triggered:
            chat = await crud.get_chat(chat_id)
            meta = chat.get("metadata") or {}
            if isinstance(meta, str):
                meta = json.loads(meta)
            meta["auto_reply_done"] = True
            await database.execute(
                chats.update()
                .where(chats.c.id == chat_id)
                .values(metadata=meta, updated_at=datetime.utcnow())
            )
            logger.info(f"[MaxAutoReply] All conditions met for chat {chat_id}")
            break


def clear_phone_number(phone_number: str) -> str:
    return "".join(filter(str.isdigit, phone_number))
