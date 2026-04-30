from typing import List

from api.nomenclature_groups.infrastructure.models.NomenclatureGroupModel import (
    NomenclatureGroupModel,
)


class INomenclatureGroupsReader:

    async def get_group_nomenclatures(self, group_id: int, cashbox_id: int):
        raise NotImplementedError()

    async def get_nomen_with_attr(self, group_id: int, cashbox_id: int):
        raise NotImplementedError()

    async def get_group_by_id(self, id: int, cashbox_id: int) -> NomenclatureGroupModel:
        raise NotImplementedError()

    async def get_all(
        self, limit: int, offset: int, cashbox_id: int
    ) -> List[NomenclatureGroupModel]:
        raise NotImplementedError()
