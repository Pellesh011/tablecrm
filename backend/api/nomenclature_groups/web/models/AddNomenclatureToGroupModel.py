from api.nomenclature_groups.web.models.BaseNomenclatureToGroupModel import (
    BaseNomenclatureToGroupModel,
)


class AddNomenclatureToGroupModel(BaseNomenclatureToGroupModel):
    is_main: bool
