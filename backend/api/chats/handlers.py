from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from api.chats.crud import get_message_file_url
from api.chats.producer import (
    ChatMessageModel,
    ChatNewChatEventModel,
    ChatTypingEventModel,
    ChatUserConnectedEventModel,
    ChatUserDisconnectedEventModel,
)
from api.chats.websocket import (
    _normalize_telegram_file_url,
    cashbox_manager,
    chat_manager,
)
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from database.db import MessageType, chats, database
from sqlalchemy import select


class ChatMessageHandler(IEventHandler):
    """Обработчик сообщений чатов из RabbitMQ"""

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        """Обработать сообщение чата из RabbitMQ"""
        try:
            chat_message = ChatMessageModel(**event)

            chat_id = chat_message.chat_id

            cashbox_id = None
            try:
                query = select([chats.c.cashbox_id]).where(chats.c.id == chat_id)
                result = await database.fetch_one(query)
                if result:
                    cashbox_id = result["cashbox_id"]
            except Exception as e:
                pass

            ws_message = {
                "type": "message",
                "message_id": chat_message.message_id_value,
                "chat_id": chat_id,
                "sender_type": chat_message.sender_type,
                "content": chat_message.content,
                "message_type": chat_message.message_type,
                "status": "DELIVERED",
                "timestamp": chat_message.timestamp,
            }
            file_types = [MessageType.DOCUMENT, MessageType.VIDEO, MessageType.VOICE]
            url = await get_message_file_url(
                chat_message.message_id_value,
                chat_message.message_type,
            )

            if chat_message.message_type == "IMAGE" and url:
                ws_message["image_url"] = _normalize_telegram_file_url(url)

            elif chat_message.message_type in file_types and url:
                ws_message["file_url"] = _normalize_telegram_file_url(url)

            await chat_manager.broadcast_to_chat(chat_id, ws_message)

            if cashbox_id:
                cashbox_message = {
                    "type": "chat_message",
                    "event": "new_message",
                    "chat_id": chat_id,
                    "message_id": chat_message.message_id_value,
                    "sender_type": chat_message.sender_type,
                    "content": chat_message.content,
                    "message_type": chat_message.message_type,
                    "timestamp": chat_message.timestamp,
                }
                if (
                    chat_message.message_type or ""
                ).upper() == "IMAGE" and ws_message.get("image_url"):
                    cashbox_message["image_url"] = ws_message["image_url"]
                if ws_message.get("file_url"):
                    cashbox_message["file_url"] = ws_message["file_url"]
                await cashbox_manager.broadcast_to_cashbox(cashbox_id, cashbox_message)

        except Exception as e:
            raise


class ChatTypingEventHandler(IEventHandler):
    """Обработчик событий печати из RabbitMQ"""

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        """Обработать событие печати из RabbitMQ"""
        try:
            typing_event = ChatTypingEventModel(**event)

            chat_id = typing_event.chat_id
            cashbox_id = None
            try:
                query = select([chats.c.cashbox_id]).where(chats.c.id == chat_id)
                result = await database.fetch_one(query)
                if result:
                    cashbox_id = result["cashbox_id"]
            except Exception as e:
                pass

            ws_message = {
                "type": "typing",
                "chat_id": chat_id,
                "user_id": typing_event.user_id,
                "user_type": typing_event.user_type,
                "user_name": getattr(typing_event, "user_name", None),
                "is_typing": typing_event.is_typing,
                "timestamp": typing_event.timestamp,
            }

            await chat_manager.broadcast_to_chat(chat_id, ws_message)

            if cashbox_id:
                cashbox_message = {
                    "type": "chat_typing",
                    "event": "typing",
                    "chat_id": chat_id,
                    "user_id": typing_event.user_id,
                    "user_type": typing_event.user_type,
                    "is_typing": typing_event.is_typing,
                    "timestamp": typing_event.timestamp,
                }
                await cashbox_manager.broadcast_to_cashbox(cashbox_id, cashbox_message)
        except Exception as e:
            raise


class ChatUserConnectedEventHandler(IEventHandler):
    """Обработчик событий подключения пользователя из RabbitMQ"""

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        """Обработать событие подключения пользователя из RabbitMQ"""
        try:
            connect_event = ChatUserConnectedEventModel(**event)

            chat_id = connect_event.chat_id

            cashbox_id = None
            try:
                query = select([chats.c.cashbox_id]).where(chats.c.id == chat_id)
                result = await database.fetch_one(query)
                if result:
                    cashbox_id = result["cashbox_id"]
            except Exception as e:
                pass

            ws_message = {
                "type": "user_connected",
                "chat_id": chat_id,
                "user_id": connect_event.user_id,
                "user_type": connect_event.user_type,
                "timestamp": connect_event.timestamp,
            }

            await chat_manager.broadcast_to_chat(chat_id, ws_message)

        except Exception as e:
            raise


class ChatUserDisconnectedEventHandler(IEventHandler):
    """Обработчик событий отключения пользователя из RabbitMQ"""

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        """Обработать событие отключения пользователя из RabbitMQ"""
        try:
            disconnect_event = ChatUserDisconnectedEventModel(**event)

            chat_id = disconnect_event.chat_id

            cashbox_id = None
            try:
                query = select([chats.c.cashbox_id]).where(chats.c.id == chat_id)
                result = await database.fetch_one(query)
                if result:
                    cashbox_id = result["cashbox_id"]
            except Exception as e:
                pass

            ws_message = {
                "type": "user_disconnected",
                "chat_id": chat_id,
                "user_id": disconnect_event.user_id,
                "user_type": disconnect_event.user_type,
                "timestamp": disconnect_event.timestamp,
            }

            await chat_manager.broadcast_to_chat(chat_id, ws_message)

            if cashbox_id:
                cashbox_message = {
                    "type": "chat_user_disconnected",
                    "event": "user_disconnected",
                    "chat_id": chat_id,
                    "user_id": disconnect_event.user_id,
                    "user_type": disconnect_event.user_type,
                    "timestamp": disconnect_event.timestamp,
                }
                await cashbox_manager.broadcast_to_cashbox(cashbox_id, cashbox_message)

        except Exception as e:
            raise


class ChatNewChatEventHandler(IEventHandler):
    """Обработчик событий создания нового чата из RabbitMQ"""

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        try:
            new_chat_event = ChatNewChatEventModel(**event)

            cashbox_message = {
                "type": "chat_message",
                "event": "new_chat",
                "chat_id": new_chat_event.chat_id,
                "timestamp": new_chat_event.timestamp,
            }
            await cashbox_manager.broadcast_to_cashbox(
                new_chat_event.cashbox_id, cashbox_message
            )
        except Exception:
            raise
