class IBookingRepository:

    async def get_nearest_time_by_status(
        self,
        current_date: int,
        booking_nomenclature_id: int,
        status: str,
        cashbox_id: int,
    ):
        raise NotImplementedError()

    async def get_by_nomenclature_id(
        self, booking_nomenclature_id: int, cashbox_id: int
    ):
        raise NotImplementedError()

    async def get_previous_by_date(
        self,
        current_start: int,
        cashbox_id: int,
        booking_nomenclature_id: int,
        status: str,
    ):
        raise NotImplementedError()

    async def get_future_by_date(
        self, current_start: int, cashbox_id: int, booking_nomenclature_id: int
    ):
        raise NotImplementedError()

    async def update_status(self, booking_id: int, status: str, cashbox_id: int):
        raise NotImplementedError()
