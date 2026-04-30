from api.nomenclature_groups.infrastructure.functions.core.IChangeMainNomenclGroupFunction import (
    IChangeMainNomenclGroupFunction,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class ChangeMainNomenclGroupView:

    def __init__(
        self,
        change_main_nomenclature_group_function: IChangeMainNomenclGroupFunction,
        nomenclatures_group_reader: INomenclatureGroupsReader,
    ):
        self.__change_main_nomenclature_group_function = (
            change_main_nomenclature_group_function
        )
        self.__nomenclatures_group_reader = nomenclatures_group_reader

    async def __call__(self, token: str, group_id: int, nomenclature_id: int):
        user = await get_user_by_token(token)

        group_info = await self.__nomenclatures_group_reader.get_group_by_id(
            id=group_id,
            cashbox_id=user.cashbox_id,
        )
        if not group_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Group not found"
            )

        await self.__change_main_nomenclature_group_function(
            group_id=group_id,
            nomen_id=nomenclature_id,
        )
