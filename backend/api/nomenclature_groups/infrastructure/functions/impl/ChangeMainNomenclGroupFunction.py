from api.nomenclature_groups.infrastructure.functions.core.IChangeMainNomenclGroupFunction import (
    IChangeMainNomenclGroupFunction,
)
from database.db import database, nomenclature_groups_value
from sqlalchemy import and_, update


class ChangeMainNomenclGroupFunction(IChangeMainNomenclGroupFunction):

    async def __call__(self, group_id: id, nomen_id: int):
        async with database.connection() as connection:
            async with connection.transaction():
                query = (
                    update(nomenclature_groups_value)
                    .where(
                        and_(
                            nomenclature_groups_value.c.group_id == group_id,
                            nomenclature_groups_value.c.is_main == True,
                        )
                    )
                    .values(is_main=False)
                )
                await database.execute(query)

                query = (
                    update(nomenclature_groups_value)
                    .where(
                        and_(
                            nomenclature_groups_value.c.group_id == group_id,
                            nomenclature_groups_value.c.nomenclature_id == nomen_id,
                        )
                    )
                    .values(is_main=True)
                )
                await database.execute(query)
