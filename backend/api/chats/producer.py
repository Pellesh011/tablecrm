import logging
import uuid
from datetime import datetime
from typing import Optional

from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage
from common.utils.ioc.ioc import ioc


class ChatMessageModel(BaseModelMessage):
    chat_id: int
    message_id_value: Optional[int] = None
    sender_type: str
    content: str
    message_type: str
    timestamp: str


class ChatTypingEventModel(BaseModelMessage):
    chat_id: int
    user_id: int
    user_type: str
    user_name: Optional[str] = None  # FIX: добавлено имя оператора
    is_typing: bool
    timestamp: str


class ChatUserConnectedEventModel(BaseModelMessage):
    chat_id: int
    user_id: int
    user_type: str
    timestamp: str


class ChatUserDisconnectedEventModel(BaseModelMessage):
    chat_id: int
    user_id: int
    user_type: str
    timestamp: str


class ChatNewChatEventModel(BaseModelMessage):
    chat_id: int
    cashbox_id: int
    timestamp: str


class ChatMessageProducer:
    """Producer для отправки сообщений чатов в RabbitMQ"""

    async def send_message(self, chat_id: int, message_data: dict):
        """Отправить сообщение в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()

            message = ChatMessageModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                message_id_value=message_data.get("message_id"),
                sender_type=message_data.get("sender_type", "OPERATOR"),
                content=message_data.get("content", ""),
                message_type=message_data.get("message_type", "TEXT"),
                timestamp=message_data.get("timestamp")
                or datetime.utcnow().isoformat(),
            )

            await rabbit_messaging.publish(message=message, routing_key="chat.messages")
            logging.getLogger(__name__).info(
                "chat_producer.send_message ok chat_id=%s message_id=%s",
                chat_id,
                message_data.get("message_id"),
            )
        except Exception as e:
            logging.getLogger(__name__).warning(
                "chat_producer.send_message failed (chat_id=%s): %s", chat_id, e
            )

    async def send_typing_event(
        self,
        chat_id: int,
        user_id: int,
        user_type: str,
        is_typing: bool,
        operator_name: Optional[str] = None,  # FIX: принимаем имя оператора
    ):
        """Отправить событие печати в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()

            event = ChatTypingEventModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                user_id=user_id,
                user_type=user_type,
                user_name=operator_name,  # FIX: передаём имя
                is_typing=is_typing,
                timestamp=datetime.utcnow().isoformat(),
            )

            await rabbit_messaging.publish(
                message=event, routing_key="chat.events.typing"
            )

        except Exception as e:
            logging.getLogger(__name__).debug(
                "chat_producer.send_typing_event failed (chat_id=%s): %s", chat_id, e
            )

    async def send_user_connected_event(
        self, chat_id: int, user_id: int, user_type: str
    ):
        """Отправить событие подключения пользователя в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()

            event = ChatUserConnectedEventModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                user_id=user_id,
                user_type=user_type,
                timestamp=datetime.utcnow().isoformat(),
            )

            await rabbit_messaging.publish(
                message=event, routing_key="chat.events.user_connected"
            )

        except Exception as e:
            pass

    async def send_user_disconnected_event(
        self, chat_id: int, user_id: int, user_type: str
    ):
        """Отправить событие отключения пользователя в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()

            event = ChatUserDisconnectedEventModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                user_id=user_id,
                user_type=user_type,
                timestamp=datetime.utcnow().isoformat(),
            )

            await rabbit_messaging.publish(
                message=event, routing_key="chat.events.user_disconnected"
            )

        except Exception as e:
            pass

    async def send_new_chat_event(self, chat_id: int, cashbox_id: int):
        """Отправить событие нового чата в очередь"""
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()

            event = ChatNewChatEventModel(
                message_id=uuid.uuid4(),
                chat_id=chat_id,
                cashbox_id=cashbox_id,
                timestamp=datetime.utcnow().isoformat(),
            )

            await rabbit_messaging.publish(
                message=event, routing_key="chat.events.new_chat"
            )
        except Exception:
            pass


chat_producer = ChatMessageProducer()
