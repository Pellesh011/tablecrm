from typing import List

from api.nomenclature.infrastructure.readers.core.INomenclatureReader import (
    INomenclatureReader,
)
from api.nomenclature_attributes.infrastructure.functions.core.IDeleteNomenclatureAttributesFunction import (
    IDeleteNomenclatureAttributesFunction,
)
from api.nomenclature_attributes.infrastructure.functions.core.IInsertNomenclatureAttributesFunction import (
    IInsertNomenclatureAttributesFunction,
)
from api.nomenclature_attributes.infrastructure.readers.core.INomenclatureAttributesReader import (
    INomenclatureAttributesReader,
)
from api.nomenclature_attributes.web.models.schemas import (
    AttributeCreateResponse,
    AttributeValueResponse,
    NomenclatureWithAttributesResponse,
)
from api.nomenclature_attributes.web.views.AddNomenclatureAttributeValueView import (
    AddNomenclatureAttributeValueView,
)
from api.nomenclature_attributes.web.views.CreateNomenclatureAttributesView import (
    CreateNomenclatureAttributesView,
)
from api.nomenclature_attributes.web.views.DeleteNomenclatureAttributeView import (
    DeleteNomenclatureAttributeView,
)
from api.nomenclature_attributes.web.views.DelNomenclatureAttributeValueView import (
    DelNomenclatureAttributeValueView,
)
from api.nomenclature_attributes.web.views.GetAttributeTypesView import (
    GetAttributeTypesView,
)
from api.nomenclature_attributes.web.views.GetNomenclatureAttributesView import (
    GetNomenclatureAttributesView,
)
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI
from starlette import status


class InstallNomenclatureAttributesWeb:

    def __call__(self, app: FastAPI):
        create_nomenclature_attributes_view = CreateNomenclatureAttributesView(
            insert_nomenclature_attributes_function=ioc.get(
                IInsertNomenclatureAttributesFunction
            )
        )

        delete_nomenclature_attributes_view = DeleteNomenclatureAttributeView(
            delete_nomenclature_attributes_function=ioc.get(
                IDeleteNomenclatureAttributesFunction
            )
        )

        add_nomenclature_attribute_value_view = AddNomenclatureAttributeValueView(
            nomenclature_reader=ioc.get(INomenclatureReader),
            nomenclature_attributes_reader=ioc.get(INomenclatureAttributesReader),
        )

        del_nomenclature_attribute_value_view = DelNomenclatureAttributeValueView(
            nomenclature_attributes_reader=ioc.get(INomenclatureAttributesReader),
            delete_nomenclature_attributes_values=ioc.get(
                IDeleteNomenclatureAttributesFunction
            ),
        )

        get_nomenclature_attributes_view = GetNomenclatureAttributesView()

        get_attribute_types_view = GetAttributeTypesView(
            nomenclature_attributes_reader=ioc.get(INomenclatureAttributesReader)
        )

        app.add_api_route(
            path="/nomenclature/attributes",
            endpoint=create_nomenclature_attributes_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            response_model=AttributeCreateResponse,
            tags=["nomenclature_attributes"],
        )

        app.add_api_route(
            path="/nomenclature/attributes",
            endpoint=delete_nomenclature_attributes_view.__call__,
            methods=["DELETE"],
            status_code=status.HTTP_200_OK,
            tags=["nomenclature_attributes"],
        )

        app.add_api_route(
            path="/nomenclature/attributes/add",
            endpoint=add_nomenclature_attribute_value_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            response_model=AttributeValueResponse,
            tags=["nomenclature_attributes"],
        )

        app.add_api_route(
            path="/nomenclature/attributes/del",
            endpoint=del_nomenclature_attribute_value_view.__call__,
            methods=["DELETE"],
            status_code=status.HTTP_200_OK,
            tags=["nomenclature_attributes"],
        )

        app.add_api_route(
            path="/nomenclature/{nomenclature_id}/attributes",
            endpoint=get_nomenclature_attributes_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=NomenclatureWithAttributesResponse,
            tags=["nomenclature_attributes"],
        )

        app.add_api_route(
            path="/nomenclature/attributes/types",
            endpoint=get_attribute_types_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=List[AttributeCreateResponse],
            tags=["nomenclature_attributes"],
        )
