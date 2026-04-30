from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from api.nomenclature_groups.web.models.ResponseGroupWithAttrModel import (
    NomenclatureAttrModel,
    NomenclatureAttrValueModel,
    NomenclatureWithAttrModel,
    ResponseGroupWithAttrModel,
)
from functions.helpers import get_user_by_token


class GetNomWithAttrFromGroupsView:

    def __init__(self, nomenclature_groups_reader: INomenclatureGroupsReader):
        self.__nomenclature_groups_reader = nomenclature_groups_reader

    async def __call__(self, token: str, group_id: int):
        user = await get_user_by_token(token)

        group_info = await self.__nomenclature_groups_reader.get_nomen_with_attr(
            group_id=group_id,
            cashbox_id=user.cashbox_id,
        )

        return ResponseGroupWithAttrModel(
            group_id=group_id,
            nomenclatures=[
                NomenclatureWithAttrModel(
                    nomenclature_id=element.nomenclature_id,
                    attribute=NomenclatureAttrModel(
                        attribute_id=element.attribute_id,
                        attribute_name=element.name,
                        attribute_alias=element.alias,
                        attribute_value=NomenclatureAttrValueModel(
                            attribute_value_id=element.attribute_value_id,
                            value=element.value,
                        ),
                    ),
                )
                for element in group_info
            ],
        )
