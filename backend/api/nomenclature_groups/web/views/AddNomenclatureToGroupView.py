from api.nomenclature.infrastructure.readers.core.INomenclatureReader import (
    INomenclatureReader,
)
from api.nomenclature_groups.infrastructure.exceptions.GroupAlreadyHaveIsMainError import (
    GroupAlreadyHaveIsMainError,
)
from api.nomenclature_groups.infrastructure.exceptions.NomenclatureIdAlreadyExistsInGroupError import (
    NomenclatureIdAlreadyExistsInGroupError,
)
from api.nomenclature_groups.infrastructure.functions.core.IAddNomenclatureToGroupFunction import (
    IAddNomenclatureToGroupFunction,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from api.nomenclature_groups.web.models.AddNomenclatureToGroupModel import (
    AddNomenclatureToGroupModel,
)
from api.nomenclature_groups.web.models.ResponseAddNomenclatureToGroup import (
    ResponseAddNomenclatureToGroup,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class AddNomenclatureToGroupView:

    def __init__(
        self,
        nomenclature_reader: INomenclatureReader,
        nomenclature_group_reader: INomenclatureGroupsReader,
        add_nomenclature_to_group_function: IAddNomenclatureToGroupFunction,
    ):
        self.__nomenclature_reader = nomenclature_reader
        self.__nomenclature_group_reader = nomenclature_group_reader
        self.__add_nomenclature_to_group_function = add_nomenclature_to_group_function

    async def __call__(self, token: str, data: AddNomenclatureToGroupModel):
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

        try:
            result_id = await self.__add_nomenclature_to_group_function(
                group_id=data.group_id,
                nomenclature_id=data.nomenclature_id,
                is_main=data.is_main,
            )
        except NomenclatureIdAlreadyExistsInGroupError as error:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail={"error": error.title}
            )
        except GroupAlreadyHaveIsMainError as error:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail={"error": error.title}
            )

        return ResponseAddNomenclatureToGroup(
            id=result_id,
            group_id=data.group_id,
            nomenclature_id=data.nomenclature_id,
            is_main=data.is_main,
        )
