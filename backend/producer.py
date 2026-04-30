import json
import uuid
from datetime import datetime

from api.apple_wallet.messages.AppleWalletCardUpdateMessage import (
    AppleWalletCardUpdateMessage,
)
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc
from database.db import database, users


async def _get_messaging():
    factory = ioc.get(IRabbitFactory)
    return await factory()


async def produce_message(body: dict) -> None:
    messaging = await _get_messaging()
    query = users.select().where(
        users.c.is_blocked == False, users.c.chat_id != body["tg_user_or_chat"]
    )
    live_users = await database.fetch_all(query=query)
    for i in live_users:
        body.update({"from_or_to": str(i.chat_id)})
        body.update({"is_blocked": i.is_blocked})
        body.update({"size": len(live_users)})
        # Используем метод publish_to_queue (должен быть добавлен в IRabbitMessaging)
        await messaging.publish_to_queue(
            queue_name="message_queue", body=json.dumps(body).encode()
        )


async def queue_notification(notification_data: dict) -> bool:
    try:
        messaging = await _get_messaging()
        # Используем publish_to_queue вместо прямого доступа к _channel
        await messaging.publish_to_queue(
            queue_name="notification_queue",
            body=json.dumps(notification_data).encode(),
            durable=True,
        )
        return True
    except Exception as e:
        print(f"Error adding notification to queue: {e}")
        return False


async def send_order_assignment_notification(
    order_id: int, role: str, user_id: int, user_name: str, links: dict = None
) -> bool:
    notification_data = {
        "type": "assignment",
        "order_id": order_id,
        "role": role,
        "user_id": user_id,
        "user_name": user_name,
        "links": links or {},
    }
    return await queue_notification(notification_data)


async def send_new_chat_notification(
    cashbox_id: int,
    chat_id: int,
    contact_name: str = None,
    channel_name: str = None,
    ad_title: str = None,
) -> bool:
    try:
        print("=== send_new_chat_notification called ===")
        print(f"cashbox_id: {cashbox_id}, chat_id: {chat_id}")
        print(
            f"contact_name: {contact_name}, channel_name: {channel_name}, ad_title: {ad_title}"
        )

        from database.db import users, users_cboxes_relation
        from sqlalchemy import and_, select

        owner_query = (
            select([users.c.chat_id])
            .select_from(
                users.join(
                    users_cboxes_relation, users.c.id == users_cboxes_relation.c.user
                )
            )
            .where(
                and_(
                    users_cboxes_relation.c.cashbox_id == cashbox_id,
                    users_cboxes_relation.c.is_owner == True,
                    users_cboxes_relation.c.status == True,
                    users.c.chat_id.isnot(None),
                )
            )
        )

        owners = await database.fetch_all(owner_query)
        recipients = [str(owner.chat_id) for owner in owners if owner.chat_id]

        print(f"Found {len(recipients)} owners: {recipients}")

        if not recipients:
            print(f"No owners found for cashbox {cashbox_id} to send chat notification")
            return False

        text = "💬 <b>Новый чат</b>\n\n"
        if contact_name:
            text += f"Контакт: {contact_name}\n"
        if channel_name:
            text += f"Канал: {channel_name}\n"
        if ad_title:
            text += f"Объявление: {ad_title}\n"
        text += f"\nID чата: {chat_id}"

        print(f"Notification text: {text}")

        notification_data = {
            "type": "segment_notification",
            "recipients": recipients,
            "text": text,
            "timestamp": datetime.now().timestamp(),
        }

        print(f"Sending notification to queue: {notification_data}")
        result = await queue_notification(notification_data)
        print(f"Notification queued: {result}")
        return result

    except Exception as e:
        print(f"Error sending new chat notification: {e}")
        import traceback

        traceback.print_exc()
        return False


async def publish_apple_wallet_pass_update(card_ids: list[int]):
    messaging = await _get_messaging()
    for card_id in card_ids:
        await messaging.publish(
            AppleWalletCardUpdateMessage(
                message_id=uuid.uuid4(),
                loyality_card_id=card_id,
            ),
            routing_key="teach_card_operation",
        )
