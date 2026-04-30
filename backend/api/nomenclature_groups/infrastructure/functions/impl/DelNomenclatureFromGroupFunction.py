from api.nomenclature_groups.infrastructure.functions.core.IDelNomenclatureFromGroupFunction import (
    IDelNomenclatureFromGroupFunction,
)
from database.db import database, nomenclature_groups_value
from sqlalchemy import and_


class DelNomenclatureFromGroupFunction(IDelNomenclatureFromGroupFunction):

    async def __call__(self, group_id: int, nomenclature_id: int):
        query = nomenclature_groups_value.delete().where(
            and_(
                nomenclature_groups_value.c.group_id == group_id,
                nomenclature_groups_value.c.nomenclature_id == nomenclature_id,
            )
        )
        await database.execute(query)
