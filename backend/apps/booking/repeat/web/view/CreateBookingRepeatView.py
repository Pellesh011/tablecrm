import uuid

from apps.booking.repeat.models.BaseBookingRepeatMessageModel import (
    BaseBookingRepeatMessage,
)
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import amo_leads, booking, booking_tags, database
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from sqlalchemy import and_, select
from starlette import status
from starlette.requests import Request


class CreateBookingRepeatView:

    def __init__(self, amqp_messaging_factory: IRabbitFactory):
        self.__amqp_messaging_factory = amqp_messaging_factory

    async def __call__(self, token: str, request: Request):
        form = await request.form()
        form_dict = dict(form)

        user = await get_user_by_token(token)

        amqp_messaging: IRabbitMessaging = await self.__amqp_messaging_factory()

        lead_id = form_dict.get("leads[add][0][id]")
        if not lead_id:
            return HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Lead Id Not Available"
            )

        query = select(amo_leads.c.id).where(
            and_(
                amo_leads.c.amo_id == int(lead_id),
                amo_leads.c.is_deleted == False,
            )
        )
        amo_lead_info = await database.fetch_one(query)
        if not amo_lead_info:
            return HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Lead Not Found"
            )

        query = (
            select(booking)
            .join(booking_tags, booking.c.id == booking_tags.c.booking_id)
            .where(
                and_(
                    booking.c.cashbox == user.cashbox_id,
                    booking_tags.c.name == f"ID_{lead_id}",
                    booking.c.is_deleted == False,
                )
            )
        )
        booking_info = await database.fetch_one(query)

        if not booking_info:
            raise HTTPException(status_code=404, detail="Booking Not Found")

        await amqp_messaging.publish(
            BaseBookingRepeatMessage(
                message_id=uuid.uuid4(),
                cashbox_id=user.cashbox_id,
                booking_id=booking_info.id,
                start_booking=booking_info.start_booking,
                end_booking=booking_info.end_booking,
                token=token,
                lead_id=amo_lead_info.id,
            ),
            routing_key="booking_repeat_tasks",
        )
