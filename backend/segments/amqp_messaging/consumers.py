import logging

from api.webhooks.producer import WebhookMessage
from api.webhooks.schemas import WebhookEventType
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.common.impl.models.QueueSettingsModel import (
    QueueSettingsModel,
)
from common.utils.ioc.ioc import ioc
from segments.amqp_messaging.handlers import SegmentStartHandler

logger = logging.getLogger(__name__)


class SegmentRabbitMQConsumer:
    def __init__(self):
        self.is_running = False
        self.rabbitmq_messaging: IRabbitMessaging = None

    async def start(self):
        if self.is_running:
            return

        self.is_running = True

        try:
            rabbit_factory: IRabbitFactory = ioc.get(IRabbitFactory)
            self.rabbitmq_messaging = await rabbit_factory()

            await self.rabbitmq_messaging.subscribe(
                WebhookMessage, SegmentStartHandler()
            )
            await self.rabbitmq_messaging.install(
                [
                    QueueSettingsModel(
                        queue_name=f"webhooks.{WebhookEventType.SEGMENTS_START.value}",
                        prefetch_count=10,
                    )
                ]
            )

        except Exception as e:
            self.is_running = False

    async def stop(self):
        if not self.is_running:
            return

        self.is_running = False


segment_consumer = SegmentRabbitMQConsumer()
