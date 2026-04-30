import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import aio_pika
from bot import bot, store_bot_message


async def send_notification_to_telegram(recipient_id: str, message: str) -> bool:
    """
    Отправляет уведомление в Telegram.

    Args:
        recipient_id: ID получателя (чата или пользователя в Telegram)
        message: Текст сообщения

    Returns:
        bool: Результат отправки (True - успешно, False - ошибка)
    """
    try:
        sent_message = await bot.send_message(
            chat_id=recipient_id, text=message, parse_mode="HTML"
        )

        await store_bot_message(
            tg_message_id=sent_message.message_id,
            tg_user_or_chat=str(recipient_id),
            from_or_to=str(bot.id),
            body=message,
        )
        return True
    except Exception as e:
        print(f"Ошибка при отправке уведомления: {e}")
        return False


async def send_order_notification(
    notification_type: str,
    order_id: int,
    order_data: Dict[str, Any],
    recipient_ids: List[str] = None,
    notification_text: str = None,
    links: Dict[str, str] = None,
) -> bool:
    """
    Отправляет уведомление о заказе через RabbitMQ

    Args:
        notification_type: Тип уведомления (general, assembly, delivery)
        order_id: ID заказа
        order_data: Данные заказа
        recipient_ids: Список ID получателей (telegram chat_id)
        notification_text: Предварительно форматированный текст уведомления (опционально)
        links: Словарь с ссылками для разных ролей

    Returns:
        bool: Успешно ли добавлено уведомление в очередь
    """
    try:
        notification_data = {
            "type": notification_type,
            "order_id": order_id,
            "recipients": recipient_ids or [],
            "text": notification_text,
            "links": links or {},
            "timestamp": datetime.now().timestamp(),
        }

        print(f"Notification data: {json.dumps(notification_data, default=str)}")

        connection = await aio_pika.connect_robust(
            host=os.getenv("RABBITMQ_HOST"),
            port=os.getenv("RABBITMQ_PORT"),
            login=os.getenv("RABBITMQ_USER"),
            password=os.getenv("RABBITMQ_PASS"),
            virtualhost=os.getenv("RABBITMQ_VHOST"),
            timeout=10,
        )

        async with connection:
            channel = await connection.channel()

            queue = await channel.declare_queue("notification_queue", durable=True)

            message = aio_pika.Message(
                body=json.dumps(notification_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await channel.default_exchange.publish(
                message, routing_key="notification_queue"
            )

        print(
            f"Order notification {notification_type} for order {order_id} queued successfully"
        )
        return True

    except Exception as e:
        print(f"Failed to send notification for order {order_id}: {str(e)}")
        return False


def format_notification_text(
    notification_type: str,
    order_data: Dict[str, Any],
    goods_data: List[Dict[str, Any]],
    contragent_data: Optional[Dict[str, Any]] = None,
    delivery_info: Optional[Dict[str, Any]] = None,
    links: Optional[Dict[str, str]] = None,
    hashes: Optional[Dict[str, str]] = None,
) -> str:
    """
    Форматирует текст уведомления о заказе в зависимости от типа уведомления

    Args:
        notification_type: Тип уведомления (general, assembly, delivery)
        order_data: Данные заказа
        goods_data: Данные товаров в заказе
        contragent_data: Данные контрагента (клиента)
        delivery_info: Информация о доставке
        links: Словарь с ссылками для разных ролей
        hashes: Словарь с хешами для разных ролей

    Returns:
        str: Отформатированный текст уведомления
    """
    order_number = order_data.get("number", "Б/Н")
    order_id = order_data.get("id", "")
    order_sum = order_data.get("sum", 0)

    paid_rubles = order_data.get("paid_rubles", 0) or 0
    paid_lt = order_data.get("paid_lt", 0) or 0

    message_parts = []

    if notification_type == "general":
        message_parts.append(f"📋 <b>Общее уведомление о заказе #{order_number}</b>")
    elif notification_type == "assembly":
        message_parts.append(f"🥡 <b>Уведомление о сборке заказа #{order_number}</b>")
    elif notification_type == "delivery":
        message_parts.append(f"🚚 <b>Уведомление о доставке заказа #{order_number}</b>")
    else:
        message_parts.append(f"📦 <b>Уведомление о заказе #{order_number}</b>")

    message_parts.append("")

    if contragent_data and notification_type in ["general", "delivery"]:
        message_parts.append("<b>👤 Клиент:</b>")
        name = contragent_data.get("name", "")
        phone = contragent_data.get("phone", "")

        if name:
            message_parts.append(f"ФИО: {name}")
        if phone:
            message_parts.append(f"Телефон: {phone}")
        message_parts.append("")

    if delivery_info and notification_type in ["general", "delivery"]:
        message_parts.append("<b>🚚 Доставка:</b>")

        address = delivery_info.get("address", "")
        if address:
            message_parts.append(f"Адрес: {address}")

        delivery_date = delivery_info.get("delivery_date")
        if delivery_date:
            if isinstance(delivery_date, int):
                delivery_date = datetime.fromtimestamp(delivery_date)
                delivery_date_str = delivery_date.strftime("%d.%m.%Y %H:%M")
            else:
                delivery_date_str = str(delivery_date)
            message_parts.append(f"Время: {delivery_date_str}")

        recipient = delivery_info.get("recipient", {})
        if recipient:
            recipient_name = recipient.get("name", "")
            recipient_surname = recipient.get("surname", "")
            recipient_phone = recipient.get("phone", "")

            if recipient_name or recipient_surname:
                full_name = f"{recipient_name} {recipient_surname}".strip()
                message_parts.append(f"ФИО получателя: {full_name}")

            if recipient_phone:
                message_parts.append(f"Телефон получателя: {recipient_phone}")

        note = delivery_info.get("note")
        if note:
            message_parts.append(f"Примечание: {note}")

        message_parts.append("")

    message_parts.append("<b>📦 Заказ:</b>")

    goods_count = len(goods_data)
    message_parts.append(f"Товаров: {goods_count}")

    message_parts.append(
        f"На сумму: {order_sum} (всего) / {paid_lt} (баллами) / {paid_rubles} (рублями)"
    )

    if links:
        message_parts.append("")
        if notification_type == "general" and links.get("general_url"):
            message_parts.append(
                f"<a href='{links['general_url']}'>Ссылка на заказ</a>"
            )
        elif notification_type == "assembly" and links.get("picker_url"):
            message_parts.append(
                f"<a href='{links['picker_url']}'>Ссылка для сборщика</a>"
            )
        elif notification_type == "delivery" and links.get("courier_url"):
            message_parts.append(
                f"<a href='{links['courier_url']}'>Ссылка для доставщика</a>"
            )

    if hashes:
        if notification_type == "general" and hashes.get("general"):
            message_parts.append(f"md5*hash1: {hashes['general'][:8]}...")
        elif notification_type == "assembly" and hashes.get("picker"):
            message_parts.append(f"md5*hash2: {hashes['picker'][:8]}...")
        elif notification_type == "delivery" and hashes.get("courier"):
            message_parts.append(f"md5*hash3: {hashes['courier'][:8]}...")

    return "\n".join(message_parts)
