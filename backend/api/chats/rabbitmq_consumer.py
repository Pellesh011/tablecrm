from api.chats.handlers import (
    ChatMessageHandler,
    ChatNewChatEventHandler,
    ChatTypingEventHandler,
    ChatUserConnectedEventHandler,
    ChatUserDisconnectedEventHandler,
)
from api.chats.producer import (
    ChatMessageModel,
    ChatNewChatEventModel,
    ChatTypingEventModel,
    ChatUserConnectedEventModel,
    ChatUserDisconnectedEventModel,
)
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.common.impl.models.QueueSettingsModel import (
    QueueSettingsModel,
)
from common.utils.ioc.ioc import ioc


class ChatRabbitMQConsumer:
    """Consumer для получения сообщений из RabbitMQ и трансляции через WebSocket"""

    def __init__(self):
        self.is_running = False
        self.rabbitmq_messaging: IRabbitMessaging = None

    async def start(self):
        """Запустить consumer для прослушивания сообщений чатов"""
        if self.is_running:
            return

        self.is_running = True

        try:
            rabbit_factory: IRabbitFactory = ioc.get(IRabbitFactory)
            self.rabbitmq_messaging = await rabbit_factory()

            await self.rabbitmq_messaging.subscribe(
                ChatMessageModel, ChatMessageHandler()
            )

            await self.rabbitmq_messaging.subscribe(
                ChatTypingEventModel, ChatTypingEventHandler()
            )

            await self.rabbitmq_messaging.subscribe(
                ChatUserConnectedEventModel, ChatUserConnectedEventHandler()
            )

            await self.rabbitmq_messaging.subscribe(
                ChatUserDisconnectedEventModel, ChatUserDisconnectedEventHandler()
            )

            await self.rabbitmq_messaging.subscribe(
                ChatNewChatEventModel, ChatNewChatEventHandler()
            )

            await self.rabbitmq_messaging.install(
                [
                    QueueSettingsModel(queue_name="chat.messages", prefetch_count=10),
                    QueueSettingsModel(
                        queue_name="chat.events.typing", prefetch_count=10
                    ),
                    QueueSettingsModel(
                        queue_name="chat.events.user_connected", prefetch_count=10
                    ),
                    QueueSettingsModel(
                        queue_name="chat.events.user_disconnected", prefetch_count=10
                    ),
                    QueueSettingsModel(
                        queue_name="chat.events.new_chat", prefetch_count=10
                    ),
                ]
            )

        except Exception as e:
            self.is_running = False

    async def stop(self):
        """Остановить consumer"""
        if not self.is_running:
            return

        self.is_running = False


chat_consumer = ChatRabbitMQConsumer()
