from typing import List, Set

from api.nomenclature_attributes.infrastructure.readers.core.INomenclatureAttributesReader import (
    INomenclatureAttributesReader,
)
from database.db import database, nomenclature_attributes, nomenclature_attributes_value
from sqlalchemy import and_, select


class NomenclatureAttributesReader(INomenclatureAttributesReader):

    async def get_types(self, limit: int, offset: int, cashbox_id: int):
        query = (
            select(nomenclature_attributes)
            .where(nomenclature_attributes.c.cashbox == cashbox_id)
            .limit(limit)
            .offset(offset)
        )
        attribute_types = await database.fetch_all(query)
        return attribute_types

    async def get_values_ids_by_ids(
        self, attribute_value_ids: List[int], cashbox_id: int
    ) -> Set[int]:
        query = (
            select(nomenclature_attributes_value.c.id)
            .join(
                nomenclature_attributes,
                nomenclature_attributes_value.c.attribute_id
                == nomenclature_attributes.c.id,
            )
            .where(
                and_(
                    nomenclature_attributes_value.c.id.in_(attribute_value_ids),
                    nomenclature_attributes.c.cashbox == cashbox_id,
                )
            )
        )
        return {record["id"] for record in await database.fetch_all(query)}

    async def get_ids_by_ids(self, ids: List[int], cashbox_id: int) -> Set[int]:
        query = select(nomenclature_attributes.c.id).where(
            and_(
                nomenclature_attributes.c.id.in_(ids),
                nomenclature_attributes.c.cashbox == cashbox_id,
            )
        )
        return {record["id"] for record in await database.fetch_all(query)}
