import asyncio
import json
import os
from asyncio import sleep

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from aiogram import Bot
from database.db import OrderStatus, database

# Опциональный импорт store_bot_message
try:
    from bot import store_bot_message
except Exception as e:
    print(f"Warning: Could not import store_bot_message: {e}")
    store_bot_message = None

bot = Bot(os.environ.get("TG_TOKEN"), parse_mode="HTML")

STATUS_TRANSLATIONS = {
    OrderStatus.received: "Получен",
    OrderStatus.processed: "Обработан",
    OrderStatus.collecting: "Собирается",
    OrderStatus.collected: "Собран",
    OrderStatus.picked: "Получен курьером",
    OrderStatus.delivered: "Доставлен",
    "received": "Получен",
    "processed": "Обработан",
    "collecting": "Собирается",
    "collected": "Собран",
    "picked": "Получен курьером",
    "delivered": "Доставлен",
}


def translate_status(status):
    """Переводит статус с английского на русский"""
    return STATUS_TRANSLATIONS.get(status, status)


async def send_notification(recipient_id: str, text: str, retry_count: int = 3) -> bool:
    """
    Отправляет уведомление через Telegram бота.

    Args:
        recipient_id: ID получателя (пользователя или чата)
        text: Текст сообщения
        retry_count: Количество попыток отправки

    Returns:
        bool: True, если отправка успешна, иначе False
    """
    print(f"Trying to send notification to {recipient_id}")

    for attempt in range(retry_count):
        try:
            print(f"Attempt {attempt + 1} to send message to {recipient_id}")
            print(text)
            sent_message = await bot.send_message(
                chat_id=recipient_id, text=text, parse_mode="HTML"
            )
            print(
                f"Message sent to {recipient_id}, message_id: {sent_message.message_id}"
            )

            try:
                if store_bot_message:
                    await store_bot_message(
                        tg_message_id=sent_message.message_id,
                        tg_user_or_chat=recipient_id,
                        from_or_to=str(bot.id),
                        body=text,
                    )
                    print(f"Message stored in database for recipient {recipient_id}")
            except Exception as db_error:
                print(f"Warning: Could not save message to database: {db_error}")

            return True

        except Exception as e:
            print(f"[ERROR] Attempt {attempt + 1}/{retry_count} failed: {e}")
            if attempt == retry_count - 1:
                return False
            await asyncio.sleep(1)


async def safe_db_connect():
    """Безопасное подключение к базе данных с обработкой ошибок"""
    try:
        print("Connecting to database...")
        await database.connect()
        print("Database connection established")
        return True
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return False


async def safe_db_disconnect():
    """Безопасное отключение от базы данных с обработкой ошибок"""
    try:
        if database.is_connected:
            print("Disconnecting from database...")
            await database.disconnect()
            print("Database connection closed")
    except Exception as e:
        print(f"Error disconnecting from database: {e}")


async def process_notification(message):
    """
    Обрабатывает уведомление из очереди.

    Args:
        message: Сообщение из очереди
    """
    try:
        print("message", message)
        data = json.loads(message)

        if data.get("type") in ["general", "assembly", "delivery"]:
            notification_type = data.get("type")
            text = data.get("text", "")
            order_id = data.get("order_id")
            links = data.get("links", {})

            if not data.get("recipients"):
                print(f"No recipients specified for order {order_id} notification")
                return

            if not text:
                print(f"No text specified for order {order_id} notification")
                return

            if links and "href" not in text:
                if notification_type == "general" and links.get("general_url"):
                    text += f"\n\n<a href='{links['general_url']}'>Открыть заказ</a>"
                elif notification_type == "assembly" and links.get("picker_url"):
                    text += (
                        f"\n\n<a href='{links['picker_url']}'>Открыть для сборщика</a>"
                    )
                elif notification_type == "delivery" and links.get("courier_url"):
                    text += (
                        f"\n\n<a href='{links['courier_url']}'>Открыть для доставки</a>"
                    )
                print(f"Added link to notification: {links}")

            for recipient_id in data.get("recipients", []):
                success = await send_notification(recipient_id, text)
                if success:
                    print(
                        f"{notification_type.capitalize()} notification for order {order_id} sent to {recipient_id}"
                    )
                else:
                    print(
                        f"Failed to send {notification_type} notification for order {order_id} to {recipient_id}"
                    )

        elif data.get("type") == "status_change":
            order_id = data.get("order_id")
            previous_status = translate_status(data.get("previous_status", ""))
            new_status = translate_status(data.get("status", ""))
            links = data.get("links", {})

            text = f"📝 <b>Изменение статуса заказа #{order_id}</b>\n\n"
            text += (
                f"Статус изменен с <b>{previous_status}</b> на <b>{new_status}</b>\n"
            )

            if "general_url" in links and links["general_url"]:
                text += f"\n<a href='{links['general_url']}'>Открыть заказ</a>"
            elif "picker_url" in links and links["picker_url"]:
                text += f"\n<a href='{links['picker_url']}'>Открыть для сборщика</a>"
            elif "courier_url" in links and links["courier_url"]:
                text += f"\n<a href='{links['courier_url']}'>Открыть для доставки</a>"

            recipients = data.get("recipients", [])

            for recipient_id in recipients:
                success = await send_notification(recipient_id, text)
                if success:
                    print(
                        f"Status change notification for order {order_id} sent to {recipient_id}"
                    )
                else:
                    print(
                        f"Failed to send status change notification for order {order_id} to {recipient_id}"
                    )

        elif data.get("type") == "assignment":
            order_id = data.get("order_id")
            role = data.get("role", "")
            user_name = data.get("user_name", "пользователь")
            links = data.get("links", {})

            text = f"👤 <b>Назначен исполнитель для заказа #{order_id}</b>\n\n"

            if role == "picker":
                text += f"<b>{user_name}</b> назначен сборщиком заказа\n"
            elif role == "courier":
                text += f"<b>{user_name}</b> назначен доставщиком заказа\n"

            if role == "picker" and "picker_url" in links and links["picker_url"]:
                text += f"\n<a href='{links['picker_url']}'>Открыть для сборки</a>"
            elif role == "courier" and "courier_url" in links and links["courier_url"]:
                text += f"\n<a href='{links['courier_url']}'>Открыть для доставки</a>"

            recipients = data.get("recipients", [])

            for recipient_id in recipients:
                success = await send_notification(recipient_id, text)
                if success:
                    print(
                        f"Assignment notification for order {order_id} sent to {recipient_id}"
                    )
                else:
                    print(
                        f"Failed to send assignment notification for order {order_id} to {recipient_id}"
                    )

        elif data.get("type") == "order_notification":
            text = data.get("text", "")
            recipients = data.get("recipients", [])

            for recipient_id in recipients:
                success = await send_notification(recipient_id, text)
                if success:
                    print(f"Legacy notification sent to {recipient_id}")
                else:
                    print(f"Failed to send legacy notification to {recipient_id}")
        elif data.get("type") == "segment_notification":

            text = data.get("text", "")
            recipients = data.get("recipients", [])
            for recipient_id in recipients:
                await sleep(0.05)
                success = await send_notification(recipient_id, text)
                if success:
                    print(f"Legacy notification sent to {recipient_id}")
                else:
                    print(f"Failed to send legacy notification to {recipient_id}")
        else:
            print(f"Unknown notification type: {data.get('type')}")
    except Exception as e:
        print(f"Error processing notification: {e}")


async def on_message(message: AbstractIncomingMessage) -> None:
    """
    Обработчик входящих сообщений из RabbitMQ.

    Args:
        message: Входящее сообщение из очереди
    """
    async with message.process():
        try:
            data = message.body.decode()
            print(f"Received message: {data[:100]}...")
            await process_notification(data)
        except Exception as e:
            print(f"Error processing message: {e}")


async def consume():
    """
    Основной цикл консьюмера для обработки уведомлений из очереди RabbitMQ.
    """
    print("Starting notification consumer...")

    # Проверяем, подключена ли БД (может быть уже подключена в main.py)
    if not database.is_connected:
        await database.connect()

    connection = None
    try:
        connection = await aio_pika.connect_robust(
            host=os.getenv("RABBITMQ_HOST"),
            port=os.getenv("RABBITMQ_PORT"),
            login=os.getenv("RABBITMQ_USER"),
            password=os.getenv("RABBITMQ_PASS"),
            virtualhost=os.getenv("RABBITMQ_VHOST"),
            timeout=10,
        )

        channel = await connection.channel()

        queue_name = "notification_queue"
        queue = await channel.declare_queue(queue_name, durable=True)

        await queue.consume(on_message)

        print(
            f"Notification consumer started. Waiting for messages in queue '{queue_name}'"
        )

        # Бесконечный цикл вместо await asyncio.Future() для работы в фоновой задаче
        while True:
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        print("Notification consumer cancelled")
        if connection and not connection.is_closed:
            await connection.close()
        raise
    except Exception as e:
        print(f"Error in notification consumer: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if connection is not None:
            try:
                await connection.close()
                print("RabbitMQ connection closed")
            except Exception as e:
                print(f"Error closing RabbitMQ connection: {e}")


if __name__ == "__main__":
    asyncio.run(consume())
