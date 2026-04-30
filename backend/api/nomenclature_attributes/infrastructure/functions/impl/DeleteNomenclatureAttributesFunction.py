from typing import List

from api.nomenclature_attributes.infrastructure.functions.core.IDeleteNomenclatureAttributesFunction import (
    IDeleteNomenclatureAttributesFunction,
)
from database.db import database, nomenclature_attributes, nomenclature_attributes_value
from sqlalchemy import and_, select


class DeleteNomenclatureAttributesFunction(IDeleteNomenclatureAttributesFunction):

    async def __call__(self, cashbox_id: int, attribute_ids: List[int]):
        async with database.connection() as connection:
            async with connection.transaction():
                subquery = select(nomenclature_attributes.c.id).where(
                    and_(
                        nomenclature_attributes.c.id.in_(attribute_ids),
                        nomenclature_attributes.c.cashbox == cashbox_id,
                    )
                )

                query = nomenclature_attributes_value.delete().where(
                    nomenclature_attributes_value.c.attribute_id.in_(subquery)
                )
                await database.execute(query)

                query = nomenclature_attributes.delete().where(
                    and_(
                        nomenclature_attributes.c.id.in_(attribute_ids),
                        nomenclature_attributes.c.cashbox == cashbox_id,
                    )
                )
                await database.execute(query)
