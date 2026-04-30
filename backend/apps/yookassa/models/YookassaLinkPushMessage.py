from typing import Optional

from apps.yookassa.models.PaymentModel import PaymentBaseModel
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class YookassaLinkPushMessage(BaseModelMessage):
    amo_install_group_id: int
    referrer: str
    docs_sales_id: Optional[int] = None
    install_id: int
    cashbox_id: int
    payment: PaymentBaseModel
