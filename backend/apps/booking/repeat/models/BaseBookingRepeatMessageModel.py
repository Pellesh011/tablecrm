from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class BaseBookingRepeatMessage(BaseModelMessage):
    token: str
    cashbox_id: int
    booking_id: int
    start_booking: int
    end_booking: int

    lead_id: int
