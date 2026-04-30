from api.nomenclature_groups.infrastructure.functions.core.ICreateNomenclatureGroupFunction import (
    ICreateNomenclatureGroupFunction,
)
from api.nomenclature_groups.web.models.BaseNomenclatureGroup import (
    BaseNomenclatureGroup,
)
from api.nomenclature_groups.web.models.ResponseCreateNomenclatureGroupModel import (
    ResponseCreateNomenclatureGroupModel,
)
from functions.helpers import get_user_by_token


class CreateNomenclatureGroupView:

    def __init__(
        self,
        create_nomenclature_group_function: ICreateNomenclatureGroupFunction,
    ):
        self.__create_nomenclature_group_function = create_nomenclature_group_function

    async def __call__(self, token: str, data: BaseNomenclatureGroup):
        user = await get_user_by_token(token)

        result = await self.__create_nomenclature_group_function(
            name=data.name, cashbox_id=user.cashbox_id
        )

        return ResponseCreateNomenclatureGroupModel(
            id=result.id, name=result.name, cashbox_id=result.cashbox_id
        )
