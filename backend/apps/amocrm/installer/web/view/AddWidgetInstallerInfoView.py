import asyncpg
from apps.amocrm.installer.infrastructure.models.InsertWidgetInstallerInfoModel import (
    InsertWidgetInstallerInfoModel,
)
from apps.amocrm.installer.infrastructure.repositories.core.IWidgetInstallerRepository import (
    IWidgetInstallerRepository,
)
from apps.amocrm.installer.web.models.AddInstallerInfoModel import AddInstallerInfoModel
from database.db import InstalledByRole
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class AddWidgetInstallerInfoView:

    def __init__(self, widget_installer_repository: IWidgetInstallerRepository):
        self.__widget_installer_repository = widget_installer_repository

    async def __call__(self, token: str, installer_info: AddInstallerInfoModel):
        user = await get_user_by_token(token)
        client_user_info = await get_user_by_token(installer_info.client_token)

        insert_data = {}

        if installer_info.installed_by_role == InstalledByRole.Partner:
            partner_user_info = await get_user_by_token(installer_info.partner_token)
            insert_data["partner_name"] = installer_info.partner_name
            insert_data["partner_cashbox"] = partner_user_info.cashbox_id
            insert_data["partner_number_phone"] = installer_info.partner_number_phone
            insert_data["client_inn"] = installer_info.client_inn

        try:
            created_installer_info = (
                await self.__widget_installer_repository.add_installer(
                    widget_installer_data=InsertWidgetInstallerInfoModel(
                        **{
                            "amo_account_id": installer_info.amo_account_id,
                            "installed_by_role": installer_info.installed_by_role,
                            "client_name": installer_info.client_name,
                            "client_cashbox": client_user_info.cashbox_id,
                            "client_number_phone": installer_info.client_number_phone,
                            **insert_data,
                        }
                    )
                )
            )
        except asyncpg.exceptions.UniqueViolationError:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Information about the installer with this amo_account_id already exists",
            )

        return created_installer_info
