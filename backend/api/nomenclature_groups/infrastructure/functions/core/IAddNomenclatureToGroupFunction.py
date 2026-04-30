class IAddNomenclatureToGroupFunction:

    async def __call__(self, group_id: int, nomenclature_id: int, is_main: bool) -> int:
        raise NotImplementedError()
