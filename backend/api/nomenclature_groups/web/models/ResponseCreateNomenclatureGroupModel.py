from api.nomenclature_groups.web.models.BaseNomenclatureGroup import (
    BaseNomenclatureGroup,
)


class ResponseCreateNomenclatureGroupModel(BaseNomenclatureGroup):
    id: int
    cashbox_id: int
