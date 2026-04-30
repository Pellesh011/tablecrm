from api.nomenclature.infrastructure.readers.core.INomenclatureReader import (
    INomenclatureReader,
)
from database.db import (
    database,
    nomenclature,
    nomenclature_groups,
    nomenclature_groups_value,
    units,
)
from sqlalchemy import select


class NomenclatureReader(INomenclatureReader):

    async def get_by_id(self, id: int, cashbox_id: int):
        query = select(nomenclature).where(
            nomenclature.c.id == id,
            nomenclature.c.cashbox == cashbox_id,
            nomenclature.c.is_deleted.is_not(True),
        )
        nomenclature_info = await database.fetch_one(query)
        return nomenclature_info

    async def get_by_id_with_prices(self, id: int, cashbox_id: int):
        query = (
            select(
                nomenclature,
                units.c.name.label("unit_name"),
                nomenclature_groups_value.c.group_id,
                nomenclature_groups.c.name.label("group_name"),
                nomenclature_groups_value.c.is_main,
            )
            .select_from(
                nomenclature.outerjoin(units, units.c.id == nomenclature.c.unit)
                .outerjoin(
                    nomenclature_groups_value,
                    nomenclature_groups_value.c.nomenclature_id == nomenclature.c.id,
                )
                .outerjoin(
                    nomenclature_groups,
                    nomenclature_groups_value.c.group_id == nomenclature_groups.c.id,
                )
            )
            .where(
                nomenclature.c.id == id,
                nomenclature.c.cashbox == cashbox_id,
                nomenclature.c.is_deleted.is_not(True),
            )
        )
        nomenclature_info = await database.fetch_one(query)
        return nomenclature_info
