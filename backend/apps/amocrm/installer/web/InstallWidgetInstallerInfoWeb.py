from apps.amocrm.installer.infrastructure.repositories.core.IWidgetInstallerRepository import (
    IWidgetInstallerRepository,
)
from apps.amocrm.installer.web.view.AddWidgetInstallerInfoView import (
    AddWidgetInstallerInfoView,
)
from apps.amocrm.installer.web.view.GetWidgetInstallerInfoView import (
    GetWidgetInstallerInfoView,
)
from apps.amocrm.installer.web.view.PatchWidgetInstallerInfoView import (
    PatchWidgetInstallerInfoView,
)
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI
from starlette import status


class InstallWidgetInstallerInfoWeb:

    def __call__(self, app: FastAPI):
        add_widget_installer_info_view = AddWidgetInstallerInfoView(
            widget_installer_repository=ioc.get(IWidgetInstallerRepository)
        )

        patch_widget_installer_info_view = PatchWidgetInstallerInfoView(
            widget_installer_repository=ioc.get(IWidgetInstallerRepository)
        )

        get_widget_installer_info_view = GetWidgetInstallerInfoView(
            widget_installer_repository=ioc.get(IWidgetInstallerRepository)
        )

        app.add_api_route(
            path="/widget_installer",
            endpoint=add_widget_installer_info_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["amocrm"],
        )

        app.add_api_route(
            path="/widget_installer",
            endpoint=patch_widget_installer_info_view.__call__,
            methods=["PATCH"],
            status_code=status.HTTP_200_OK,
            tags=["amocrm"],
        )

        app.add_api_route(
            path="/widget_installer/{amo_account_id}/",
            endpoint=get_widget_installer_info_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["amocrm"],
        )
