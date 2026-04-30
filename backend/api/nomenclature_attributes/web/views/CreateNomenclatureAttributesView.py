from api.nomenclature_attributes.infrastructure.exceptions.NomenclatureAttributeNameAlreadyExistError import (
    NomenclatureAttributeNameAlreadyExistError,
)
from api.nomenclature_attributes.infrastructure.functions.core.IInsertNomenclatureAttributesFunction import (
    IInsertNomenclatureAttributesFunction,
)
from api.nomenclature_attributes.web.models.schemas import (
    AttributeCreate,
    AttributeCreateResponse,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class CreateNomenclatureAttributesView:

    def __init__(
        self,
        insert_nomenclature_attributes_function: IInsertNomenclatureAttributesFunction,
    ):
        self.__insert_nomenclature_attributes_function = (
            insert_nomenclature_attributes_function
        )

    async def __call__(
        self,
        token: str,
        attribute_data: AttributeCreate,
    ):
        user = await get_user_by_token(token)

        try:
            nomenclature_attribute_id = (
                await self.__insert_nomenclature_attributes_function(
                    name=attribute_data.name,
                    alias=attribute_data.alias,
                    cashbox_id=user.cashbox_id,
                )
            )
        except NomenclatureAttributeNameAlreadyExistError as error:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=error.title
            )

        return AttributeCreateResponse(
            id=nomenclature_attribute_id,
            name=attribute_data.name,
            alias=attribute_data.alias,
        )
