class IBookingNomenclatureRepository:

    async def get_by_id(self, cashbox: int, nomenclature_id: int):
        raise NotImplementedError()
