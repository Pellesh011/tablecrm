import logging
import uuid

from api.webhooks.schemas import WebhookEventType
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage
from common.utils.ioc.ioc import ioc

logger = logging.getLogger(__file__)


class WebhookMessage(BaseModelMessage):
    data: dict


class WebhookProducer:
    def __init__(self, event: WebhookEventType, data: dict):
        self.event = event
        self.data = data

    async def produce(self):
        try:
            rabbit_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()

            message = WebhookMessage(message_id=uuid.uuid4(), data=self.data)

            await rabbit_messaging.publish(
                message=message, routing_key="webhooks.%s" % self.event.value
            )
            logger.info(
                "webhook_producer.%s ok message_id=%s",
                self.event.value,
                message.message_id,
            )

        except Exception as e:
            pass
