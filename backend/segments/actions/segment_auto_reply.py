import json
import logging
from datetime import datetime
from random import randint
from typing import Any, Dict, List, Optional

from api.chats.crud import mark_qr_registration
from database.db import contragents, database, loyality_cards, segments

logger = logging.getLogger(__name__)


async def _save_system_message(chat_id: int, text: str) -> None:
    """Сохранить системное сообщение в чат (видно оператору)."""
    try:
        from database.db import chat_messages, database

        await database.execute(
            chat_messages.insert().values(
                chat_id=chat_id,
                sender_type="OPERATOR",
                message_type="SYSTEM",
                content=text,
                status="DELIVERED",
                source="auto_reply",
            )
        )
        logger.info(
            f"[AutoReply] Системное сообщение сохранено: chat_id={chat_id}, text={text[:60]}"
        )
    except Exception as e:
        logger.warning(f"[AutoReply] Не удалось сохранить системное сообщение: {e}")


CALLBACK_PREFIX = "auto_reply"


async def _get_auto_reply_segments(cashbox_id: int, channel_id: int) -> List[Dict]:
    """Вернуть все активные сегменты с auto_reply.enabled=True для данного канала."""
    rows = await database.fetch_all(
        segments.select().where(
            segments.c.cashbox_id == cashbox_id,
            segments.c.is_deleted.isnot(True),
            segments.c.is_archived == False,
        )
    )
    result = []
    for row in rows:
        raw = row.get("auto_reply")
        if not raw:
            continue
        ar = json.loads(raw) if isinstance(raw, str) else dict(raw)
        if not ar.get("enabled"):
            continue
        if ar.get("channel_id") != channel_id:
            continue
        result.append({"segment_id": row["id"], "config": ar})
    logger.debug(
        f"[AutoReply] Found {len(result)} auto-reply segments for cashbox {cashbox_id} channel {channel_id}"
    )  # ADDED
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


def _parse_meta(raw) -> Dict:
    if not raw:
        return {}
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return dict(raw) if isinstance(raw, dict) else {}


async def _check_conditions(
    conditions: List[Dict], cashbox_id: int, phone: Optional[str]
) -> List[Dict]:
    triggered = []
    contragent = await _find_contragent_by_phone(phone, cashbox_id)
    logger.debug(
        f"[AutoReply] _check_conditions: phone={phone}, contragent exists={contragent is not None}"
    )

    for cond in conditions:
        ctype = cond.get("type")
        if ctype == "not_registered":
            if not phone or contragent is None:
                triggered.append(cond)
                logger.debug(
                    f"[AutoReply] condition not_registered triggered (phone={phone}, contragent={contragent})"
                )
        elif ctype == "no_loyalty_card":
            if contragent is not None and not await _has_loyalty_card(
                contragent.id, cashbox_id
            ):
                triggered.append(cond)
                logger.debug(
                    f"[AutoReply] condition no_loyalty_card triggered for contragent {contragent.id}"
                )
    return triggered


def _build_reply_markup(triggered: List[Dict], segment_id: int) -> Optional[Dict]:
    """
    Строим reply_markup для Telegram.

    not_registered  → ReplyKeyboardMarkup с request_contact
    no_loyalty_card → InlineKeyboardMarkup с callback_data
    """
    inline_rows: List[List[Dict]] = []
    reply_btns: List[Dict] = []

    for cond in triggered:
        ctype = cond.get("type")
        text = cond.get("button_text", "Действие")

        if ctype == "not_registered":
            reply_btns.append({"text": text, "request_contact": True})
        elif ctype == "no_loyalty_card":
            cb = f"{CALLBACK_PREFIX}:register_loyalty:{segment_id}"
            inline_rows.append([{"text": text, "callback_data": cb}])

    if reply_btns:
        return {
            "keyboard": [[btn] for btn in reply_btns],
            "resize_keyboard": True,
            "one_time_keyboard": True,
        }
    if inline_rows:
        return {"inline_keyboard": inline_rows}
    return None


async def handle_incoming_message(
    *,
    bot_token: str,
    channel_id: int,
    cashbox_id: int,
    telegram_chat_id: str,
    chat_id: int,
    chat_metadata: Dict,
    phone: Optional[str],
) -> None:
    logger.info(
        f"[AutoReply] handle_incoming_message called for chat_id={chat_id}, phone={phone}, channel_id={channel_id}"
    )

    try:
        if chat_metadata.get("auto_reply_done"):
            logger.info(f"[AutoReply] chat_id={chat_id} already done, skipping")
            return

        ar_segments = await _get_auto_reply_segments(cashbox_id, channel_id)
        logger.info(
            f"[AutoReply] Found {len(ar_segments)} auto-reply segments for channel {channel_id}"
        )

        if not ar_segments:
            logger.info("[AutoReply] No segments, exiting")
            return

        from api.chats.telegram.telegram_client import send_message

        for item in ar_segments:
            segment_id = item["segment_id"]
            config = item["config"]
            logger.info(f"[AutoReply] Checking segment {segment_id}, config: {config}")

            conditions = config.get("conditions", [])
            if not conditions:
                logger.info(f"[AutoReply] Segment {segment_id} has no conditions, skip")
                continue

            sent_key = f"auto_reply_sent_{segment_id}"
            if config.get("send_only_once", True) and chat_metadata.get(sent_key):
                logger.info(
                    f"[AutoReply] Segment {segment_id} already sent (flag {sent_key}), skip"
                )
                continue

            triggered = await _check_conditions(conditions, cashbox_id, phone)
            logger.info(
                f"[AutoReply] Segment {segment_id} triggered conditions: {[c['type'] for c in triggered]}"
            )

            if not triggered:
                continue

            greeting = (
                config.get("greeting_text")
                or "Добро пожаловать! Для продолжения выберите действие:"
            )
            markup = _build_reply_markup(triggered, segment_id)
            if not markup:
                logger.warning(f"[AutoReply] Segment {segment_id} no markup built")
                continue

            sent = await send_message(
                token=bot_token,
                chat_id=telegram_chat_id,
                text=greeting,
                reply_markup=markup,
            )

            chat_metadata[sent_key] = True
            tg_msg_id = sent.get("message_id")
            if tg_msg_id:
                chat_metadata[f"auto_reply_msg_id_{segment_id}"] = tg_msg_id

            # Сохраняем уведомление оператору что кнопки были отправлены
            btn_names = [c.get("button_text", "?") for c in triggered]
            notify_sent = f"Пользователю отправлена кнопка: {', '.join(btn_names)}"
            await _save_system_message(chat_id, notify_sent)
            logger.info(
                f"[AutoReply] segment={segment_id} → кнопки отправлены chat_id={chat_id} tg={telegram_chat_id} msg_id={tg_msg_id}: {btn_names}"
            )

    except Exception as exc:
        logger.exception(f"[AutoReply] handle_incoming_message error: {exc}")


async def handle_auto_reply_callback(
    *,
    bot_token: str,
    callback_query: Dict[str, Any],
    channel_id: int,
    cashbox_id: int,
    chat_id: int,
    telegram_chat_id: str,
    phone: Optional[str],
) -> bool:
    """
    Возвращает True если callback обработан нами, False — чужой.
    """
    data: str = callback_query.get("data") or ""
    if not data.startswith(CALLBACK_PREFIX + ":"):
        return False

    parts = data.split(":")
    if len(parts) < 3:
        return False

    action = parts[1]
    try:
        segment_id = int(parts[2])
    except ValueError:
        return False

    from api.chats.telegram.telegram_client import answer_callback_query, delete_message

    callback_id = callback_query.get("id")
    message = callback_query.get("message") or {}
    tg_message_id = message.get("message_id")

    # ADDED: логируем полученный callback
    logger.info(
        f"[AutoReply] Callback received: action={action}, segment_id={segment_id}, chat_id={chat_id}"
    )

    try:
        if action == "register_loyalty":
            success_text = "Карта лояльности зарегистрирована! ✅"
            error_text = None
            try:
                await _action_register_loyalty(cashbox_id=cashbox_id, phone=phone)
            except Exception as e:
                error_text = f"Ошибка регистрации карты: {str(e)}"
                success_text = "Ошибка при регистрации ✗"
                logger.error(f"[AutoReply] register_loyalty error: {e}")
            await answer_callback_query(bot_token, callback_id, text=success_text)
            # Сохраняем сообщение в чат для оператора
            notify_text = (
                error_text or "Пользователь зарегистрировал карту лояльности ✅"
            )
            await _save_system_message(chat_id, notify_text)
            logger.info(
                f"[AutoReply] Кнопка 'register_loyalty' нажата: chat_id={chat_id}, phone={phone}, success={not error_text}"
            )
        else:
            await answer_callback_query(bot_token, callback_id, text="Готово!")
            notify_text = f"Пользователь нажал кнопку (действие: {action})"
            await _save_system_message(chat_id, notify_text)
            logger.info(
                f"[AutoReply] Неизвестное действие '{action}': chat_id={chat_id}"
            )

        if tg_message_id:
            try:
                await delete_message(
                    token=bot_token,
                    chat_id=telegram_chat_id,
                    message_id=tg_message_id,
                )
                logger.info(
                    f"[AutoReply] Deleted message {tg_message_id} after callback"
                )
            except Exception as del_exc:
                logger.warning(f"[AutoReply] delete_message failed: {del_exc}")

        await _maybe_mark_done(
            cashbox_id=cashbox_id,
            channel_id=channel_id,
            chat_id=chat_id,
            segment_id=segment_id,
            phone=phone,
        )

        logger.info(
            f"[AutoReply] callback '{action}' обработан: "
            f"chat_id={chat_id} segment={segment_id}"
        )
        return True

    except Exception as exc:
        logger.exception(f"[AutoReply] handle_auto_reply_callback: {exc}")
        try:
            await answer_callback_query(bot_token, callback_id, text="Произошла ошибка")
        except Exception:
            pass
        return True


async def handle_contact_received(
    *,
    bot_token: str,
    cashbox_id: int,
    telegram_chat_id: str,
    phone: str,
    first_name: Optional[str],
    last_name: Optional[str],
    chat_id: int,
) -> None:
    from api.chats import crud
    from api.chats.telegram.telegram_client import send_message
    from database.db import chat_contacts

    logger.info(f"[AutoReply] Contact received: phone={phone}, chat_id={chat_id}")

    chat = await crud.get_chat(chat_id)
    chat_contact_id = chat.get("chat_contact_id")
    if not chat_contact_id:
        logger.warning(f"No chat_contact_id for chat {chat_id}")
        return

    try:
        contragent = await _find_contragent_by_phone(phone, cashbox_id)
        if not contragent:
            ts = int(datetime.now().timestamp())
            full_name = " ".join(filter(None, [first_name, last_name])) or ""

            stmt = (
                contragents.insert()
                .values(
                    name=full_name,
                    external_id="",
                    phone=phone,
                    inn="",
                    description="",
                    cashbox=cashbox_id,
                    is_deleted=False,
                    created_at=ts,
                    updated_at=ts,
                )
                .returning(contragents.c.id)
            )
            contragent_id = await database.execute(stmt)
            logger.info(
                f"[AutoReply] Создан контрагент phone={phone} cashbox={cashbox_id} id={contragent_id}"
            )
        else:
            contragent_id = contragent.id

        await database.execute(
            chat_contacts.update()
            .where(chat_contacts.c.id == chat_contact_id)
            .values(contragent_id=contragent_id)
        )
        logger.info(
            f"[AutoReply] Контрагент {contragent_id} привязан к chat_contact {chat_contact_id}"
        )

        await send_message(
            token=bot_token,
            chat_id=telegram_chat_id,
            text="Вы успешно зарегистрированы! ✅",
            reply_markup={"remove_keyboard": True},
        )

        notify_msg = f"Пользователь зарегистрирован: {phone}"
        await mark_qr_registration(chat_id)
        await _save_system_message(chat_id, notify_msg)
        logger.info(
            f"[AutoReply] handle_contact_received: сохранено системное сообщение для chat_id={chat_id}"
        )

    except Exception as exc:
        logger.exception(f"[AutoReply] handle_contact_received: {exc}")


async def _action_register_loyalty(cashbox_id: int, phone: Optional[str]) -> None:
    if not phone:
        raise ValueError("Нет телефона — невозможно создать карту лояльности")
    contragent = await _find_contragent_by_phone(phone, cashbox_id)
    if not contragent:
        raise ValueError(f"Контрагент с телефоном {phone} не найден")
    if await _has_loyalty_card(contragent.id, cashbox_id):
        logger.info(
            f"[AutoReply] Карта лояльности уже существует для контрагента {contragent.id}"
        )
        return
    await database.execute(
        loyality_cards.insert().values(
            contragent_id=contragent.id,
            cashbox_id=cashbox_id,
            card_number=randint(0, 9_223_372_036_854_775),
            balance=0,
            is_deleted=False,
            apple_wallet_advertisement="",
        )
    )
    logger.info(f"[AutoReply] Создана карта лояльности для контрагента {contragent.id}")


async def _maybe_mark_done(
    cashbox_id: int,
    channel_id: int,
    chat_id: int,
    segment_id: int,
    phone: Optional[str],
) -> None:
    """Если все условия выполнены — пометить чат как auto_reply_done."""
    from api.chats import crud
    from database.db import chats

    try:
        ar_segments = await _get_auto_reply_segments(cashbox_id, channel_id)
        for item in ar_segments:
            if item["segment_id"] != segment_id:
                continue
            conditions = item["config"].get("conditions", [])
            triggered = await _check_conditions(conditions, cashbox_id, phone)
            if not triggered:
                chat = await crud.get_chat(chat_id)
                meta = _parse_meta(chat.get("metadata"))
                meta["auto_reply_done"] = True
                await database.execute(
                    chats.update()
                    .where(chats.c.id == chat_id)
                    .values(metadata=meta, updated_at=datetime.utcnow())
                )
                logger.info(
                    f"[AutoReply] Все условия выполнены: chat_id={chat_id} segment={segment_id}"
                )
    except Exception as exc:
        logger.warning(f"[AutoReply] _maybe_mark_done: {exc}")
