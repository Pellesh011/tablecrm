from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class CreatePurchaseAutoExpenseMessage(BaseModelMessage):
    token: str
    cashbox_id: int
    purchase_id: int
