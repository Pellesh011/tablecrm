from api.nomenclature_groups.infrastructure.functions.core.IPatchNomenclatureGroupFunction import (
    IPatchNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from api.nomenclature_groups.web.models.PatchNomenclatureGroupModel import (
    PatchNomenclatureGroupModel,
)
from api.nomenclature_groups.web.models.ResponsePatchNomenclatureGroupModel import (
    ResponsePatchNomenclatureGroupModel,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class PatchNomenclatureGroupView:

    def __init__(
        self,
        nomenclature_groups_reader: INomenclatureGroupsReader,
        patch_nomenclature_group_function: IPatchNomenclatureGroupFunction,
    ):
        self.__nomenclature_groups_reader = nomenclature_groups_reader
        self.__patch_nomenclature_group_function = patch_nomenclature_group_function

    async def __call__(self, token: str, id: int, data: PatchNomenclatureGroupModel):
        user = await get_user_by_token(token)

        group_info = await self.__nomenclature_groups_reader.get_group_by_id(
            id=id,
            cashbox_id=user.cashbox_id,
        )

        if not group_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Group not found"
            )

        await self.__patch_nomenclature_group_function(
            group_id=id, name=data.name, cashbox_id=user.cashbox_id
        )

        return ResponsePatchNomenclatureGroupModel(
            id=id, cashbox_id=user.cashbox_id, name=data.name
        )
