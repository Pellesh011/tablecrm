from api.nomenclature_groups.infrastructure.models.GroupModelWithNomenclaturesModel import (
    GroupModelWithNomenclaturesModel,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class GetNomenclatureGroupByIdView:

    def __init__(
        self,
        nomenclature_group_reader: INomenclatureGroupsReader,
    ):
        self.__nomenclature_group_reader = nomenclature_group_reader

    async def __call__(self, token: str, group_id: int):
        user = await get_user_by_token(token)

        group_info = await self.__nomenclature_group_reader.get_group_by_id(
            cashbox_id=user.cashbox_id,
            id=group_id,
        )

        if not group_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Group not found"
            )

        nomenclatures = await self.__nomenclature_group_reader.get_group_nomenclatures(
            cashbox_id=user.cashbox_id,
            group_id=group_id,
        )

        return GroupModelWithNomenclaturesModel(
            id=group_info.id,
            name=group_info.name,
            cashbox_id=group_info.cashbox_id,
            nomenclatures=nomenclatures,
        )
