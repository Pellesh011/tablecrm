from apps.booking.booking.infrastructure.repositories.core.IBookingRepository import (
    IBookingRepository,
)
from database.db import booking, booking_nomenclature, database
from sqlalchemy import and_, asc, desc, func, select, update


class BookingRepository(IBookingRepository):

    async def get_nearest_time_by_status(
        self,
        current_date: int,
        booking_nomenclature_id: int,
        status: str,
        cashbox_id: int,
    ):
        difference = func.abs(booking.c.start_booking - current_date)

        query = (
            select(booking)
            .join(
                booking_nomenclature, booking.c.id == booking_nomenclature.c.booking_id
            )
            .where(
                and_(
                    booking.c.cashbox == cashbox_id,
                    booking_nomenclature.c.nomenclature_id == booking_nomenclature_id,
                    booking.c.status_booking == status,
                )
            )
            .order_by(difference.asc())
            .limit(1)
        )

        closest_booking = await database.fetch_one(query)
        return closest_booking

    async def get_by_nomenclature_id(
        self, booking_nomenclature_id: int, cashbox_id: int
    ):

        query = (
            select(booking)
            .join(
                booking_nomenclature, booking.c.id == booking_nomenclature.c.booking_id
            )
            .where(
                and_(
                    booking.c.cashbox == cashbox_id,
                    booking_nomenclature.c.nomenclature_id == booking_nomenclature_id,
                )
            )
            .limit(1)
        )
        booking_info = await database.fetch_one(query)
        return booking_info

    async def get_previous_by_date(
        self,
        current_start: int,
        cashbox_id: int,
        booking_nomenclature_id: int,
        status: str,
    ):
        previous_query = (
            select(booking)
            .join(
                booking_nomenclature, booking.c.id == booking_nomenclature.c.booking_id
            )
            .where(
                and_(
                    booking.c.start_booking < current_start,
                    booking.c.cashbox == cashbox_id,
                    booking_nomenclature.c.nomenclature_id == booking_nomenclature_id,
                    booking.c.status_booking == status,
                )
            )
            .order_by(desc(booking.c.start_booking))
            .limit(1)
        )
        previous_booking = await database.fetch_one(previous_query)
        return None if not previous_booking else previous_booking.id

    async def get_future_by_date(
        self, current_start: int, cashbox_id: int, booking_nomenclature_id: int
    ):
        next_query = (
            select(booking)
            .join(
                booking_nomenclature, booking.c.id == booking_nomenclature.c.booking_id
            )
            .where(
                and_(
                    booking.c.start_booking > current_start,
                    booking.c.cashbox == cashbox_id,
                    booking_nomenclature.c.nomenclature_id == booking_nomenclature_id,
                )
            )
            .order_by(asc(booking.c.start_booking))
            .limit(1)
        )
        next_booking = await database.fetch_one(next_query)
        return None if not next_booking else next_booking.id

    async def update_status(self, booking_id: int, status: str, cashbox_id: int):
        query = (
            update(booking)
            .where(and_(booking.c.id == booking_id, booking.c.cashbox == cashbox_id))
            .values(status_booking=status)
        )
        await database.execute(query)
