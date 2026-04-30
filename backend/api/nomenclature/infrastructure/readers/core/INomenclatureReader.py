class INomenclatureReader:

    async def get_by_id(self, id: int, cashbox_id: int):
        raise NotImplementedError()

    async def get_by_id_with_prices(self, id: int, cashbox_id: int):
        raise NotImplementedError()
