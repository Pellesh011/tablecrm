from typing import List

from api.nomenclature_attributes.infrastructure.functions.core.IDeleteNomenclatureAttributesValuesFunction import (
    IDeleteNomenclatureAttributesValuesFunction,
)
from database.db import database, nomenclature_attributes_value


class DeleteNomenclatureAttributesValuesFunction(
    IDeleteNomenclatureAttributesValuesFunction
):

    async def __call__(self, cashbox_id: int, attribute_value_ids: List[int]):
        query = nomenclature_attributes_value.delete().where(
            nomenclature_attributes_value.c.id.in_(attribute_value_ids),
        )
        await database.execute(query)
