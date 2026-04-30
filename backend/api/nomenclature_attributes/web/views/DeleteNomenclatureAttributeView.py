from typing import List

from api.nomenclature_attributes.infrastructure.functions.core.IDeleteNomenclatureAttributesFunction import (
    IDeleteNomenclatureAttributesFunction,
)
from functions.helpers import get_user_by_token


class DeleteNomenclatureAttributeView:

    def __init__(
        self,
        delete_nomenclature_attributes_function: IDeleteNomenclatureAttributesFunction,
    ):
        self.__delete_nomenclature_attributes_function = (
            delete_nomenclature_attributes_function
        )

    async def __call__(self, token: str, attribute_ids: List[int]):
        user = await get_user_by_token(token)

        await self.__delete_nomenclature_attributes_function(
            cashbox_id=user.cashbox_id,
            attribute_ids=attribute_ids,
        )
