from typing import Optional

from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class NewLeadBaseModelMessage(BaseModelMessage):
    lead_name: str
    price: Optional[int]
    status_id: int
    contact_id: Optional[int]
    account_link: str
    act_link: str
    nomenclature: str
    start_period: int
    end_period: int
    booking_id: int

    docs_sales_id: int
    cashbox_id: int
