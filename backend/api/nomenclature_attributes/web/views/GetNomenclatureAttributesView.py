from api.nomenclature_attributes.web.models import schemas
from database.db import (
    database,
    nomenclature,
    nomenclature_attributes,
    nomenclature_attributes_value,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from sqlalchemy import and_, select


class GetNomenclatureAttributesView:

    async def __call__(self, token: str, nomenclature_id: int):
        user = await get_user_by_token(token)

        check_nomenclature_query = select(nomenclature).where(
            nomenclature.c.id == nomenclature_id,
            nomenclature.c.cashbox == user.cashbox_id,
        )
        res = await database.fetch_one(check_nomenclature_query)
        if not res:
            raise HTTPException(
                status_code=404,
                detail=f"Номенклатура с ID {nomenclature_id} не найдена.",
            )

        query = (
            select(
                nomenclature_attributes.c.id,
                nomenclature_attributes.c.name,
                nomenclature_attributes.c.alias,
                nomenclature_attributes_value.c.value.label("attribute_values"),
            )
            .select_from(
                nomenclature_attributes_value.join(
                    nomenclature_attributes,
                    nomenclature_attributes_value.c.attribute_id
                    == nomenclature_attributes.c.id,
                ).join(
                    nomenclature,
                    nomenclature_attributes_value.c.nomenclature_id
                    == nomenclature.c.id,
                )
            )
            .where(
                and_(
                    nomenclature_attributes_value.c.nomenclature_id == nomenclature_id,
                    nomenclature.c.cashbox == user.cashbox_id,
                    nomenclature_attributes.c.cashbox == user.cashbox_id,
                )
            )
        )

        results = await database.fetch_all(query)

        return schemas.NomenclatureWithAttributesResponse(
            nomenclature_id=nomenclature_id,
            attributes=[
                schemas.AttributeResponse(
                    id=result.id,
                    name=result.name,
                    alias=result.alias,
                    values=result.attribute_values,
                )
                for result in results
            ],
        )
