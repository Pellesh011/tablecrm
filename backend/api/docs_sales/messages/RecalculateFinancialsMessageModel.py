from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class RecalculateFinancialsMessageModel(BaseModelMessage):
    cashbox_id: int
    token: str
