from typing import List

from api.nomenclature_attributes.infrastructure.functions.core.IDeleteNomenclatureAttributesValuesFunction import (
    IDeleteNomenclatureAttributesValuesFunction,
)
from api.nomenclature_attributes.infrastructure.readers.core.INomenclatureAttributesReader import (
    INomenclatureAttributesReader,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class DelNomenclatureAttributeValueView:

    def __init__(
        self,
        nomenclature_attributes_reader: INomenclatureAttributesReader,
        delete_nomenclature_attributes_values: IDeleteNomenclatureAttributesValuesFunction,
    ):
        self.__nomenclature_attributes_reader = nomenclature_attributes_reader
        self.__delete_nomenclature_attributes_values = (
            delete_nomenclature_attributes_values
        )

    async def __call__(
        self,
        token: str,
        attribute_value_ids: List[int],
    ):
        user = await get_user_by_token(token)

        value_ids = await self.__nomenclature_attributes_reader.get_values_ids_by_ids(
            attribute_value_ids=attribute_value_ids, cashbox_id=user.cashbox_id
        )

        for attribute_value_id in attribute_value_ids:
            if attribute_value_id not in value_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Attribute value not found",
                )

        await self.__delete_nomenclature_attributes_values(
            attribute_value_ids=attribute_value_ids, cashbox_id=user.cashbox_id
        )
