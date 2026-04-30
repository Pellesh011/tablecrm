from datetime import datetime

from api.marketplace.service.qr_service.constants import QrEntityTypes
from pydantic import BaseModel


class QRResolveResponse(BaseModel):
    """Ответ QR-резолвера"""

    type: QrEntityTypes  # "product" или "location"
    entity: dict  # Данные товара или локации
    qr_hash: str
    resolved_at: datetime
