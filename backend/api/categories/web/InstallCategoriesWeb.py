from api.categories import schemas
from api.categories.web.views.GetAllCategoriesView import GetAllCategoriesView
from api.categories.web.views.GetCategoriesChildrenByIdView import (
    GetCategoriesChildrenByIdView,
)
from api.categories.web.views.GetCategoriesTreeView import GetCategoriesTreeView
from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI
from starlette import status


class InstallCategoriesWeb:

    def __call__(self, app: FastAPI):
        get_all_categories_view = GetAllCategoriesView(
            s3_factory=ioc.get(IS3ServiceFactory)
        )

        get_categories_tree_view = GetCategoriesTreeView(
            s3_factory=ioc.get(IS3ServiceFactory)
        )

        get_categories_children_by_id_view = GetCategoriesChildrenByIdView()

        app.add_api_route(
            path="/categories/",
            endpoint=get_all_categories_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.CategoryListGet,
            tags=["categories"],
        )

        app.add_api_route(
            path="/categories_tree/",
            endpoint=get_categories_tree_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.CategoryTreeGet,
            tags=["categories"],
        )

        app.add_api_route(
            path="/categories_tree/{idx}/children/",
            endpoint=get_categories_children_by_id_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=schemas.CategoryTreeGet,
            tags=["categories"],
        )
