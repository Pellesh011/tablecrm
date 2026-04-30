class IDeleteNomenclatureGroupFunction:

    async def __call__(self, group_id: int, cashbox_id: int) -> None:
        raise NotImplementedError()
