from apps.booking.repeat.web.view.CreateBookingRepeatView import CreateBookingRepeatView
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI
from starlette import status


class InstallBookingRepeatWeb:

    def __call__(self, app: FastAPI):
        create_booking_repeat_view = CreateBookingRepeatView(
            amqp_messaging_factory=ioc.get(IRabbitFactory)
        )

        app.add_api_route(
            path="/booking/amo/repeat",
            endpoint=create_booking_repeat_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["booking"],
        )
