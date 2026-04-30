from api.nomenclature import schemas
from api.nomenclature.infrastructure.readers.impl.NomenclatureReader import (
    NomenclatureReader,
)
from api.nomenclature.web.views.GetNomenclatureByIdView import GetNomenclatureByIdView
from api.prices.infrastructure.readers.impl.PricesReader import PricesReader
from fastapi import FastAPI
from starlette import status


class InstallNomenclatureWeb:

    def __call__(self, app: FastAPI):
        get_nomenclature_by_id = GetNomenclatureByIdView(
            nomenclature_reader=NomenclatureReader(),
            prices_reader=PricesReader(),
        )

        app.add_api_route(
            path="/nomenclature/{idx}/",
            endpoint=get_nomenclature_by_id.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.NomenclatureGet,
            tags=["manufacturers"],
        )
