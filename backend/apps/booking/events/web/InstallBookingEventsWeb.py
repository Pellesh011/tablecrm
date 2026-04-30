from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI
from starlette import status

from ...booking.infrastructure.repositories.core.IBookingRepository import (
    IBookingRepository,
)
from ...nomenclature.infrastructure.repositories.core.IBookingNomenclatureRepository import (
    IBookingNomenclatureRepository,
)
from ..functions.impl.EventFilterConverterFunction import EventFilterConverterFunction
from ..functions.impl.EventsGetFunction import EventsGetFunction
from ..infrastructure.repositories.core.IBookingEventsRepository import (
    IBookingEventsRepository,
)
from ..infrastructure.services.impl.BookingEventsService import BookingEventsService
from .view.AddImageBookingEventView import AddImageBookingEventView
from .view.CreateBookingEventsView import CreateBookingEventsView
from .view.DeleteBookingEventByIdView import DeleteBookingEventByIdView
from .view.DeleteImageBookingEventView import DeleteImageBookingEventView
from .view.GetBookingEventsView import GetBookingEventsView
from .view.PatchBookingEventsView import PatchBookingEventsView


class InstallBookingEventsWeb:

    def __call__(self, app: FastAPI):
        create_booking_events_view = CreateBookingEventsView(
            booking_events_service=BookingEventsService(
                booking_events_repository=ioc.get(IBookingEventsRepository),
                booking_nomenclature_repository=ioc.get(IBookingNomenclatureRepository),
            ),
            booking_repository=ioc.get(IBookingRepository),
        )

        add_image_booking_event_view = AddImageBookingEventView(
            booking_events_service=BookingEventsService(
                booking_events_repository=ioc.get(IBookingEventsRepository),
                booking_nomenclature_repository=ioc.get(IBookingNomenclatureRepository),
            )
        )

        delete_booking_event_by_id = DeleteBookingEventByIdView(
            booking_events_service=BookingEventsService(
                booking_events_repository=ioc.get(IBookingEventsRepository),
                booking_nomenclature_repository=ioc.get(IBookingNomenclatureRepository),
            )
        )

        delete_image_booking_event = DeleteImageBookingEventView(
            booking_events_service=BookingEventsService(
                booking_events_repository=ioc.get(IBookingEventsRepository),
                booking_nomenclature_repository=ioc.get(IBookingNomenclatureRepository),
            )
        )

        get_booking_events_view = GetBookingEventsView(
            booking_events_service=BookingEventsService(
                booking_events_repository=ioc.get(IBookingEventsRepository),
                booking_nomenclature_repository=ioc.get(IBookingNomenclatureRepository),
            ),
            event_filter_converter=EventFilterConverterFunction(),
            event_get_function=EventsGetFunction(s3_factory=ioc.get(IS3ServiceFactory)),
        )

        patch_booking_events_view = PatchBookingEventsView(
            booking_events_service=BookingEventsService(
                booking_events_repository=ioc.get(IBookingEventsRepository),
                booking_nomenclature_repository=ioc.get(IBookingNomenclatureRepository),
            )
        )

        app.add_api_route(
            path="/booking/events",
            endpoint=create_booking_events_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["booking"],
        )

        app.add_api_route(
            path="/booking/events/photos",
            endpoint=add_image_booking_event_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["booking"],
        )

        app.add_api_route(
            path="/booking/events",
            endpoint=delete_booking_event_by_id.__call__,
            methods=["DELETE"],
            status_code=status.HTTP_200_OK,
            tags=["booking"],
        )

        app.add_api_route(
            path="/booking/events/photos",
            endpoint=delete_image_booking_event.__call__,
            methods=["DELETE"],
            status_code=status.HTTP_200_OK,
            tags=["booking"],
        )

        app.add_api_route(
            path="/booking/events/search",
            endpoint=get_booking_events_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["booking"],
        )

        app.add_api_route(
            path="/booking/events",
            endpoint=patch_booking_events_view.__call__,
            methods=["PATCH"],
            status_code=status.HTTP_200_OK,
            tags=["booking"],
        )
