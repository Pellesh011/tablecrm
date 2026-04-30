from apps.amocrm.installer.infrastructure.models.InsertWidgetInstallerInfoModel import (
    InsertWidgetInstallerInfoModel,
)
from apps.amocrm.installer.infrastructure.models.PatchWidgetInstallerInfoModel import (
    PatchWidgetInstallerInfoModel,
)
from apps.amocrm.installer.infrastructure.models.ResponseGetByIdWidgetInstallerInfoModel import (
    ResponseGetByIdWidgetInstallerInfoModel,
)
from apps.amocrm.installer.infrastructure.models.ResponseInsertWidgetInstallerInfoModel import (
    ResponseInsertWidgetInstallerInfoModel,
)
from apps.amocrm.installer.infrastructure.models.ResponsePatchWidgetInstallerInfoModel import (
    ResponsePatchWidgetInstallerInfoModel,
)


class IWidgetInstallerRepository:

    async def get_installer(
        self, amo_account_id: int, cashbox_id: int
    ) -> ResponseGetByIdWidgetInstallerInfoModel:
        raise NotImplementedError()

    async def add_installer(
        self, widget_installer_data: InsertWidgetInstallerInfoModel
    ) -> ResponseInsertWidgetInstallerInfoModel:
        raise NotImplementedError()

    async def patch_installer(
        self,
        amo_account_id: int,
        client_cashbox_id: int,
        widget_installer_data: PatchWidgetInstallerInfoModel,
    ) -> ResponsePatchWidgetInstallerInfoModel:
        raise NotImplementedError()
