from api.nomenclature_groups.infrastructure.functions.core.IPatchNomenclatureGroupFunction import (
    IPatchNomenclatureGroupFunction,
)
from database.db import database, nomenclature_groups
from sqlalchemy import and_


class PatchNomenclatureGroupFunction(IPatchNomenclatureGroupFunction):

    async def __call__(self, group_id: int, name: str, cashbox_id: int):
        query = (
            nomenclature_groups.update()
            .where(
                and_(
                    nomenclature_groups.c.id == group_id,
                    nomenclature_groups.c.cashbox == cashbox_id,
                )
            )
            .values(name=name)
        )
        await database.execute(query)
