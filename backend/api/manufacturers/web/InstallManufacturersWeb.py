from api.manufacturers import schemas
from api.manufacturers.web.views.CreateManufacturersView import CreateManufacturersView
from api.manufacturers.web.views.DeleteManufacturersView import DeleteManufacturersView
from api.manufacturers.web.views.GetManufacturerByIdView import GetManufacturerByIdView
from api.manufacturers.web.views.GetManufacturersView import GetManufacturersView
from api.manufacturers.web.views.PatchManufacturersView import PatchManufacturersView
from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI
from starlette import status


class InstallManufacturersWeb:

    def __call__(self, app: FastAPI):
        get_manufacturer_by_id_view = GetManufacturerByIdView()

        get_manufacturers_view = GetManufacturersView(
            s3_factory=ioc.get(IS3ServiceFactory)
        )

        create_manufacturers_view = CreateManufacturersView()

        patch_manufacturers_view = PatchManufacturersView()

        delete_manufacturers_view = DeleteManufacturersView()

        app.add_api_route(
            path="/manufacturers/{idx}/",
            endpoint=get_manufacturer_by_id_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.Manufacturer,
            tags=["nomenclature"],
        )

        app.add_api_route(
            path="/manufacturers/",
            endpoint=get_manufacturers_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.ManufacturerListGet,
            tags=["nomenclature"],
        )

        app.add_api_route(
            path="/manufacturers/",
            endpoint=create_manufacturers_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.ManufacturerList,
            tags=["nomenclature"],
        )

        app.add_api_route(
            path="/manufacturers/{idx}/",
            endpoint=patch_manufacturers_view.__call__,
            methods=["PATCH"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.Manufacturer,
            tags=["nomenclature"],
        )

        app.add_api_route(
            path="/manufacturers/{idx}/",
            endpoint=delete_manufacturers_view.__call__,
            methods=["DELETE"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.Manufacturer,
            tags=["nomenclature"],
        )
