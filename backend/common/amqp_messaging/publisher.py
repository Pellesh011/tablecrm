# common/amqp_messaging/publisher.py
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.utils.ioc.ioc import ioc


async def get_publisher() -> IRabbitMessaging:
    factory = ioc.get(IRabbitFactory)
    return await factory()
