class IPatchNomenclatureGroupFunction:

    async def __call__(self, group_id: int, name: str, cashbox_id: int):
        raise NotImplementedError()
