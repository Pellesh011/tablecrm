from typing import List

from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class RecalculateLoyaltyPointsMessageModel(BaseModelMessage):
    loyalty_card_ids: List[int]
