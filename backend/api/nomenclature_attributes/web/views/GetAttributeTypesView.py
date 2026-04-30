from typing import Annotated

from api.nomenclature_attributes.infrastructure.readers.core.INomenclatureAttributesReader import (
    INomenclatureAttributesReader,
)
from api.nomenclature_attributes.web.models.schemas import AttributeCreateResponse
from fastapi import Query
from functions.helpers import get_user_by_token


class GetAttributeTypesView:

    def __init__(self, nomenclature_attributes_reader: INomenclatureAttributesReader):
        self.__nomenclature_attributes_reader = nomenclature_attributes_reader

    async def __call__(
        self,
        token: str,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=100)] = 100,
    ):
        user = await get_user_by_token(token)

        results = await self.__nomenclature_attributes_reader.get_types(
            limit=limit, offset=offset, cashbox_id=user.cashbox_id
        )

        return [
            AttributeCreateResponse(id=result.id, name=result.name, alias=result.alias)
            for result in results
        ]
