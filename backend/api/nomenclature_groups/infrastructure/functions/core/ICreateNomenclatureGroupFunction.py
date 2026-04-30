from api.nomenclature_groups.infrastructure.models import NomenclatureGroupModel


class ICreateNomenclatureGroupFunction:

    async def __call__(self, name: str, cashbox_id: int) -> NomenclatureGroupModel:
        raise NotImplementedError()
