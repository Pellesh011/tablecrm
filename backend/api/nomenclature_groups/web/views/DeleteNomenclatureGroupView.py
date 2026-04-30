from api.nomenclature_groups.infrastructure.functions.core.IDeleteNomenclatureGroupFunction import (
    IDeleteNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class DeleteNomenclatureGroupView:

    def __init__(
        self,
        nomenclature_groups_reader: INomenclatureGroupsReader,
        delete_nomenclature_group_function: IDeleteNomenclatureGroupFunction,
    ):
        self.__nomenclature_groups_reader = nomenclature_groups_reader
        self.__delete_nomenclature_group_function = delete_nomenclature_group_function

    async def __call__(self, token: str, id: int):
        user = await get_user_by_token(token)

        group_info = await self.__nomenclature_groups_reader.get_group_by_id(
            id=id,
            cashbox_id=user.cashbox_id,
        )

        if not group_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Group not found"
            )

        await self.__delete_nomenclature_group_function(
            group_id=id, cashbox_id=user.cashbox_id
        )
