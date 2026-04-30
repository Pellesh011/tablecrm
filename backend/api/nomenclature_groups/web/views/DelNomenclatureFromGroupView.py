from api.nomenclature.infrastructure.readers.core.INomenclatureReader import (
    INomenclatureReader,
)
from api.nomenclature_groups.infrastructure.functions.core.IDelNomenclatureFromGroupFunction import (
    IDelNomenclatureFromGroupFunction,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from api.nomenclature_groups.web.models.DelNomenclatureFromGroupModel import (
    DelNomenclatureFromGroupModel,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class DelNomenclatureFromGroupView:

    def __init__(
        self,
        nomenclature_reader: INomenclatureReader,
        nomenclature_group_reader: INomenclatureGroupsReader,
        del_nomenclature_from_group_function: IDelNomenclatureFromGroupFunction,
    ):
        self.__nomenclature_reader = nomenclature_reader
        self.__nomenclature_group_reader = nomenclature_group_reader
        self.__del_nomenclature_from_group_function = (
            del_nomenclature_from_group_function
        )

    async def __call__(self, token: str, data: DelNomenclatureFromGroupModel):
        user = await get_user_by_token(token)

        nomenclature = await self.__nomenclature_reader.get_by_id(
            id=data.nomenclature_id, cashbox_id=user.cashbox_id
        )

        if not nomenclature:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Nomenclature not found"
            )

        group_info = await self.__nomenclature_group_reader.get_group_by_id(
            id=data.group_id, cashbox_id=user.cashbox_id
        )

        if not group_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Group not found"
            )

        await self.__del_nomenclature_from_group_function(
            group_id=data.group_id,
            nomenclature_id=data.nomenclature_id,
        )
