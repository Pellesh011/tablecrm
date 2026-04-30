class IInsertNomenclatureAttributesFunction:

    async def __call__(self, name: str, alias: str, cashbox_id: int):
        raise NotImplementedError()
