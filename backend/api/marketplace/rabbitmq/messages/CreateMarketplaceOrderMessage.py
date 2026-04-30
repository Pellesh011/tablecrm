from typing import Any, Dict, List, Optional

from api.docs_sales.schemas import DeliveryInfoSchema
from api.marketplace.service.orders_service.schemas import (
    CreateOrderUtm,
    MarketplaceOrderGood,
)
from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage
from pydantic import Field


class OrderGoodMessage(MarketplaceOrderGood):
    organization_id: int


class CreateMarketplaceOrderMessage(BaseModelMessage):
    marketplace_order_id: int
    phone: str
    cashbox_id: int
    contragent_id: int
    goods: List[OrderGoodMessage]
    delivery_info: DeliveryInfoSchema
    utm: Optional[CreateOrderUtm] = None
    additional_data: List[Dict[str, Any]] = Field(default_factory=list)
