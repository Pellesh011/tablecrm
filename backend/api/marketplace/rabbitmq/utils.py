from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc


def get_rabbitmq_factory():
    return ioc.get(IRabbitFactory)
