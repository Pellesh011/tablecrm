from typing import Optional

from pydantic import BaseModel


class PaymentAnalytics(BaseModel):
    article_id: Optional[int]
    article_image: Optional[str]
    article_emoji: Optional[str]
    article_name: Optional[str]
    type: str
    sum: int
    percentage: float


class PaymentsAnalytics(BaseModel):
    __root__: Optional[list[PaymentAnalytics]]
