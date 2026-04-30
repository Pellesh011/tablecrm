from typing import List, Set


class INomenclatureAttributesReader:

    async def get_types(self, limit: int, offset: int, cashbox_id: int):
        raise NotImplementedError()

    async def get_values_ids_by_ids(
        self, attribute_value_ids: List[int], cashbox_id: int
    ) -> Set[int]:
        raise NotImplementedError()

    async def get_ids_by_ids(self, ids: List[int], cashbox_id: int) -> Set[int]:
        raise NotImplementedError()
