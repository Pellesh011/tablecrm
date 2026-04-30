from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class AppleWalletCardUpdateMessage(BaseModelMessage):
    loyality_card_id: int
