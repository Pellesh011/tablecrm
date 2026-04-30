from typing import List


class IDeleteNomenclatureAttributesValuesFunction:

    async def __call__(self, cashbox_id: int, attribute_value_ids: List[int]):
        raise NotImplementedError()
