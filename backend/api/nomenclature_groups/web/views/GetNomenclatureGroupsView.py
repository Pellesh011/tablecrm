from typing import Annotated

from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from fastapi import Query
from functions.helpers import get_user_by_token


class GetNomenclatureGroupsView:

    def __init__(
        self,
        nomenclature_group_reader: INomenclatureGroupsReader,
    ):
        self.__nomenclature_group_reader = nomenclature_group_reader

    async def __call__(
        self,
        token: str,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=100)] = 100,
    ):
        user = await get_user_by_token(token)

        return await self.__nomenclature_group_reader.get_all(
            limit=limit,
            offset=offset,
            cashbox_id=user.cashbox_id,
        )
