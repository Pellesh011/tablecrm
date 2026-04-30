from typing import List

from api.nomenclature.infrastructure.readers.core.INomenclatureReader import (
    INomenclatureReader,
)
from api.nomenclature_groups.infrastructure.functions.core.IAddNomenclatureToGroupFunction import (
    IAddNomenclatureToGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.IChangeMainNomenclGroupFunction import (
    IChangeMainNomenclGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.ICreateNomenclatureGroupFunction import (
    ICreateNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.IDeleteNomenclatureGroupFunction import (
    IDeleteNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.IDelNomenclatureFromGroupFunction import (
    IDelNomenclatureFromGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.IPatchNomenclatureGroupFunction import (
    IPatchNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.models.GroupModelWithNomenclaturesModel import (
    GroupModelWithNomenclaturesModel,
)
from api.nomenclature_groups.infrastructure.models.NomenclatureGroupModel import (
    NomenclatureGroupModel,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from api.nomenclature_groups.web.models.ResponseAddNomenclatureToGroup import (
    ResponseAddNomenclatureToGroup,
)
from api.nomenclature_groups.web.models.ResponseCreateNomenclatureGroupModel import (
    ResponseCreateNomenclatureGroupModel,
)
from api.nomenclature_groups.web.models.ResponsePatchNomenclatureGroupModel import (
    ResponsePatchNomenclatureGroupModel,
)
from api.nomenclature_groups.web.views.AddNomenclatureToGroupView import (
    AddNomenclatureToGroupView,
)
from api.nomenclature_groups.web.views.ChangeMainNomenclGroupView import (
    ChangeMainNomenclGroupView,
)
from api.nomenclature_groups.web.views.CreateNomenclatureGroupView import (
    CreateNomenclatureGroupView,
)
from api.nomenclature_groups.web.views.DeleteNomenclatureGroupView import (
    DeleteNomenclatureGroupView,
)
from api.nomenclature_groups.web.views.DelNomenclatureFromGroupView import (
    DelNomenclatureFromGroupView,
)
from api.nomenclature_groups.web.views.GetNomenclatureGroupByIdView import (
    GetNomenclatureGroupByIdView,
)
from api.nomenclature_groups.web.views.GetNomenclatureGroupsView import (
    GetNomenclatureGroupsView,
)
from api.nomenclature_groups.web.views.GetNomWithAttrFromGroupsView import (
    GetNomWithAttrFromGroupsView,
)
from api.nomenclature_groups.web.views.PatchNomenclatureGroupView import (
    PatchNomenclatureGroupView,
)
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI
from starlette import status


class InstallNomenclatureGroupsWeb:

    def __call__(self, app: FastAPI):
        add_nomenclature_to_group_view = AddNomenclatureToGroupView(
            nomenclature_reader=ioc.get(INomenclatureReader),
            nomenclature_group_reader=ioc.get(INomenclatureGroupsReader),
            add_nomenclature_to_group_function=ioc.get(IAddNomenclatureToGroupFunction),
        )

        create_nomenclature_group_view = CreateNomenclatureGroupView(
            create_nomenclature_group_function=ioc.get(
                ICreateNomenclatureGroupFunction
            ),
        )

        delete_nomenclature_group_view = DeleteNomenclatureGroupView(
            nomenclature_groups_reader=ioc.get(INomenclatureGroupsReader),
            delete_nomenclature_group_function=ioc.get(
                IDeleteNomenclatureGroupFunction
            ),
        )

        patch_nomenclature_group_view = PatchNomenclatureGroupView(
            nomenclature_groups_reader=ioc.get(INomenclatureGroupsReader),
            patch_nomenclature_group_function=ioc.get(IPatchNomenclatureGroupFunction),
        )

        get_nom_with_attr_from_groups_view = GetNomWithAttrFromGroupsView(
            nomenclature_groups_reader=ioc.get(INomenclatureGroupsReader),
        )

        del_nomenclature_from_group_view = DelNomenclatureFromGroupView(
            nomenclature_reader=ioc.get(INomenclatureReader),
            nomenclature_group_reader=ioc.get(INomenclatureGroupsReader),
            del_nomenclature_from_group_function=ioc.get(
                IDelNomenclatureFromGroupFunction
            ),
        )

        get_nomenclatures_groups_view = GetNomenclatureGroupsView(
            nomenclature_group_reader=ioc.get(INomenclatureGroupsReader),
        )

        get_nomenclature_group_by_id_view = GetNomenclatureGroupByIdView(
            nomenclature_group_reader=ioc.get(INomenclatureGroupsReader),
        )

        change_main_nomenclature_group_view = ChangeMainNomenclGroupView(
            nomenclatures_group_reader=ioc.get(INomenclatureGroupsReader),
            change_main_nomenclature_group_function=ioc.get(
                IChangeMainNomenclGroupFunction
            ),
        )

        app.add_api_route(
            path="/nomenclature/group",
            endpoint=create_nomenclature_group_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            response_model=ResponseCreateNomenclatureGroupModel,
            tags=["nomenclature_groups"],
        )

        app.add_api_route(
            path="/nomenclature/group",
            endpoint=get_nomenclatures_groups_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=List[NomenclatureGroupModel],
            tags=["nomenclature_groups"],
        )

        app.add_api_route(
            path="/nomenclature/group/{group_id}",
            endpoint=get_nomenclature_group_by_id_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=GroupModelWithNomenclaturesModel,
            tags=["nomenclature_groups"],
        )

        app.add_api_route(
            path="/nomenclature/group/{group_id}/select_main",
            endpoint=change_main_nomenclature_group_view.__call__,
            methods=["PATCH"],
            status_code=status.HTTP_200_OK,
            tags=["nomenclature_groups"],
        )

        app.add_api_route(
            path="/nomenclature/group",
            endpoint=delete_nomenclature_group_view.__call__,
            methods=["DELETE"],
            status_code=status.HTTP_200_OK,
            tags=["nomenclature_groups"],
        )

        app.add_api_route(
            path="/nomenclature/group",
            endpoint=patch_nomenclature_group_view.__call__,
            methods=["PATCH"],
            status_code=status.HTTP_200_OK,
            response_model=ResponsePatchNomenclatureGroupModel,
            tags=["nomenclature_groups"],
        )

        app.add_api_route(
            path="/nomenclature/group/add",
            endpoint=add_nomenclature_to_group_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            responses={status.HTTP_200_OK: {"model": ResponseAddNomenclatureToGroup}},
            tags=["nomenclature_groups"],
        )

        app.add_api_route(
            path="/nomenclature/group/del",
            endpoint=del_nomenclature_from_group_view.__call__,
            methods=["DELETE"],
            status_code=status.HTTP_200_OK,
            tags=["nomenclature_groups"],
        )

        app.add_api_route(
            path="/nomenclature/group/{group_id}/attr",
            endpoint=get_nom_with_attr_from_groups_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["nomenclature_groups"],
        )
