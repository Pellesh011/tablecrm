from typing import List

from api.nomenclature_groups.infrastructure.models.GroupModelWithNomenclaturesModel import (
    Nomenclature,
)
from api.nomenclature_groups.infrastructure.models.NomenclatureGroupModel import (
    NomenclatureGroupModel,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from database.db import (
    database,
    nomenclature,
    nomenclature_attributes,
    nomenclature_attributes_value,
    nomenclature_groups,
    nomenclature_groups_value,
)
from sqlalchemy import and_, select


class NomenclatureGroupsReader(INomenclatureGroupsReader):

    async def get_group_nomenclatures(self, group_id: int, cashbox_id: int):
        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature_groups_value.c.is_main,
            )
            .select_from(nomenclature_groups_value)
            .join(
                nomenclature,
                nomenclature_groups_value.c.nomenclature_id == nomenclature.c.id,
            )
            .join(
                nomenclature_groups,
                nomenclature_groups_value.c.group_id == nomenclature_groups.c.id,
            )
            .where(
                and_(
                    nomenclature_groups.c.cashbox == cashbox_id,
                    nomenclature_groups.c.id == group_id,
                )
            )
        )
        results = await database.fetch_all(query)
        return [
            Nomenclature(
                id=result.id,
                name=result.name,
                is_main=result.is_main,
            )
            for result in results
        ]

    async def get_nomen_with_attr(self, group_id: int, cashbox_id: int):
        query = (
            select(
                nomenclature.c.id.label("nomenclature_id"),
                nomenclature_attributes_value.c.id.label("attribute_value_id"),
                nomenclature_attributes_value.c.value,
                nomenclature_attributes.c.id.label("attribute_id"),
                nomenclature_attributes.c.name,
                nomenclature_attributes.c.alias,
            )
            .select_from(nomenclature)
            .join(
                nomenclature_groups_value,
                nomenclature_groups_value.c.nomenclature_id == nomenclature.c.id,
            )
            .join(
                nomenclature_attributes_value,
                nomenclature.c.id == nomenclature_attributes_value.c.nomenclature_id,
            )
            .join(
                nomenclature_attributes,
                nomenclature_attributes.c.id
                == nomenclature_attributes_value.c.attribute_id,
            )
            .where(
                and_(
                    nomenclature_groups_value.c.group_id == group_id,
                    nomenclature.c.cashbox == cashbox_id,
                )
            )
        )

        result = await database.fetch_all(query)
        return result

    async def get_group_by_id(self, id: int, cashbox_id: int) -> NomenclatureGroupModel:
        query = nomenclature_groups.select().where(
            and_(
                nomenclature_groups.c.id == id,
                nomenclature_groups.c.cashbox == cashbox_id,
            )
        )
        result = await database.fetch_one(query)
        return NomenclatureGroupModel(
            id=result.id,
            cashbox_id=result.cashbox,
            name=result.name,
        )

    async def get_all(
        self, limit: int, offset: int, cashbox_id: int
    ) -> List[NomenclatureGroupModel]:
        query = (
            nomenclature_groups.select()
            .where(nomenclature_groups.c.cashbox == cashbox_id)
            .limit(limit)
            .offset(offset)
        )
        results = await database.fetch_all(query)
        return [
            NomenclatureGroupModel(
                id=result.id,
                cashbox_id=result.cashbox,
                name=result.name,
            )
            for result in results
        ]
