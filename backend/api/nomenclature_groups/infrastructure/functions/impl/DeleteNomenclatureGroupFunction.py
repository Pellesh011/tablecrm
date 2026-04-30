from api.nomenclature_groups.infrastructure.functions.core.IDeleteNomenclatureGroupFunction import (
    IDeleteNomenclatureGroupFunction,
)
from database.db import database, nomenclature_groups, nomenclature_groups_value
from sqlalchemy import and_


class DeleteNomenclatureGroupFunction(IDeleteNomenclatureGroupFunction):

    async def __call__(self, group_id: int, cashbox_id: int) -> None:
        async with database.connection() as connection:
            async with connection.transaction():
                query = nomenclature_groups_value.delete().where(
                    and_(nomenclature_groups_value.c.group_id == group_id)
                )
                await database.execute(query)

                query = nomenclature_groups.delete().where(
                    and_(nomenclature_groups.c.id == group_id)
                )
                await database.execute(query)
