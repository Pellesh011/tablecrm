from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from api.docs_sales.schemas import DeliveryInfoSchema
from api.marketplace.schemas import BaseMarketplaceUtm, UtmEntityType
from pydantic import BaseModel, Field


class MarketplaceOrderGood(BaseModel):
    nomenclature_id: int
    warehouse_id: Optional[int] = None  # ID помещения
    quantity: int = 1  # Количество товара
    is_from_cart: Optional[bool] = False


class MarketplaceOrderRequest(BaseModel):
    """Запрос на создание заказа маркетплейса"""

    goods: List[MarketplaceOrderGood]
    delivery: DeliveryInfoSchema
    contragent_phone: str
    contragent_first_name: Optional[str] = None
    contragent_last_name: Optional[str] = None
    # order_type: str = "self"  # Тип заказа: self, other, corporate, gift, proxy
    client_lat: Optional[float] = None
    client_lon: Optional[float] = None
    ref_user: Optional[str] = None
    additional_data: List[Dict[str, Any]] = Field(default_factory=list)


class MarketplaceOrderResponse(BaseModel):
    """Ответ на создание заказа маркетплейса"""

    # order_id: str
    # status: str
    message: str
    processing_time_ms: Optional[int] = None
    cart_cleared: bool = True  # Флаг для фронтенда, что корзина будет очищена
    # estimated_delivery: Optional[str] = None
    # cashbox_assignments: Optional[List[dict]] = None


class CreateOrderUtm(BaseMarketplaceUtm):
    entity_type: UtmEntityType = UtmEntityType.docs_sales


class LastDeliveryAddressResponse(BaseModel):
    found: bool
    address: Optional[str] = None
    delivery_date: Optional[int] = None
    delivery_price: Optional[float] = None
    recipient_name: Optional[str] = None
    recipient_surname: Optional[str] = None
    recipient_phone: Optional[str] = None
    note: Optional[str] = None
    payment_method: Optional[str] = None
    contragent_first_name: Optional[str] = None
    contragent_last_name: Optional[str] = None


class OrderStatusLabel(str, Enum):
    awaiting_payment = "awaiting_payment"
    collecting = "collecting"
    shipped = "shipped"
    ready_for_pickup = "ready_for_pickup"
    delivered = "delivered"
    processing = "processing"
    error = "error"


class OrderSortBy(str, Enum):
    created_at = "created_at"
    updated_at = "updated_at"
    delivery_date = "delivery_date"


ORDER_STATUS_MAP = {
    "received": OrderStatusLabel.awaiting_payment,
    "processed": OrderStatusLabel.awaiting_payment,
    "collecting": OrderStatusLabel.collecting,
    "collected": OrderStatusLabel.collecting,
    "picked": OrderStatusLabel.shipped,
    "delivered": OrderStatusLabel.ready_for_pickup,
    "success": OrderStatusLabel.delivered,
    "closed": OrderStatusLabel.delivered,
}


class OrderGoodItem(BaseModel):
    nomenclature_id: int
    name: str
    quantity: float
    price: float
    photo: Optional[str] = None
    unit: Optional[str] = None


class OrderItemResponse(BaseModel):
    id: int
    docs_sales_id: Optional[int] = None
    number: Optional[str] = None
    status: OrderStatusLabel
    track_number: Optional[str] = None
    delivery_company: Optional[str] = None
    created_at: datetime
    goods: List[OrderGoodItem] = Field(default_factory=list)
    address: Optional[str] = None
    delivery_date: Optional[int] = None
    delivery_price: Optional[float] = None
    recipient_name: Optional[str] = None
    recipient_surname: Optional[str] = None
    recipient_phone: Optional[str] = None
    note: Optional[str] = None
    payment_method: Optional[str] = None
    total_sum: Optional[float] = None


class OrderListResponse(BaseModel):
    result: List[OrderItemResponse]
    count: int
    page: int
    size: int
