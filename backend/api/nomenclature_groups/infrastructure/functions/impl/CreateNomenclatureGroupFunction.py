from api.nomenclature_groups.infrastructure.functions.core.ICreateNomenclatureGroupFunction import (
    ICreateNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.models.NomenclatureGroupModel import (
    NomenclatureGroupModel,
)
from database.db import database, nomenclature_groups


class CreateNomenclatureGroupFunction(ICreateNomenclatureGroupFunction):

    async def __call__(self, name: str, cashbox_id: int) -> NomenclatureGroupModel:
        query = (
            nomenclature_groups.insert()
            .values(name=name, cashbox=cashbox_id)
            .returning(nomenclature_groups.c.id)
        )
        result = await database.fetch_one(query=query)
        return NomenclatureGroupModel(id=result.id, name=name, cashbox_id=cashbox_id)
