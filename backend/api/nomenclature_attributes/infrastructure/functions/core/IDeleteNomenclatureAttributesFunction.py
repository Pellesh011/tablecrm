from typing import List


class IDeleteNomenclatureAttributesFunction:

    async def __call__(self, cashbox_id: int, attribute_ids: List[int]):
        raise NotImplementedError()
