class IDelNomenclatureFromGroupFunction:

    async def __call__(self, group_id: int, nomenclature_id: int):
        raise NotImplementedError()
