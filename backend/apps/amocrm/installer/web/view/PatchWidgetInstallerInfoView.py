from apps.amocrm.installer.infrastructure.models.PatchWidgetInstallerInfoModel import (
    PatchWidgetInstallerInfoModel,
)
from apps.amocrm.installer.infrastructure.repositories.core.IWidgetInstallerRepository import (
    IWidgetInstallerRepository,
)
from apps.amocrm.installer.web.models.PatchInstallerInfoModel import (
    PatchInstallerInfoModel,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class PatchWidgetInstallerInfoView:

    def __init__(self, widget_installer_repository: IWidgetInstallerRepository):
        self.__widget_installer_repository = widget_installer_repository

    async def __call__(self, token: str, patch_installer_info: PatchInstallerInfoModel):
        user = await get_user_by_token(token)

        installer_info = await self.__widget_installer_repository.get_installer(
            amo_account_id=patch_installer_info.amo_account_id,
            cashbox_id=user.cashbox_id,
        )

        if not installer_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "Client Cashbox not foung"},
            )

        patch_data = {}

        if patch_installer_info.client_token:

            client_user_info = await get_user_by_token(
                patch_installer_info.client_token
            )
            patch_data["client_cashbox"] = client_user_info.cashbox_id

        if patch_installer_info.client_number_phone:
            patch_data["client_number_phone"] = patch_installer_info.client_number_phone

        if patch_installer_info.client_name:
            patch_data["client_name"] = patch_installer_info.client_name

        if installer_info.partner_cashbox:

            if patch_installer_info.partner_name:
                patch_data["partner_name"] = patch_installer_info.partner_name

            if patch_installer_info.partner_number_phone:
                patch_data["partner_number_phone"] = (
                    patch_installer_info.partner_number_phone
                )

            if patch_installer_info.client_inn:
                patch_data["client_inn"] = patch_installer_info.client_inn

            if patch_installer_info.partner_token:
                partner_user_info = await get_user_by_token(
                    patch_installer_info.partner_token
                )
                patch_data["partner_cashbox"] = partner_user_info.cashbox_id

        response_patch = await self.__widget_installer_repository.patch_installer(
            amo_account_id=patch_installer_info.amo_account_id,
            client_cashbox_id=installer_info.client_cashbox,
            widget_installer_data=PatchWidgetInstallerInfoModel(**patch_data),
        )

        return {
            "amo_account_id": patch_installer_info.amo_account_id,
            **response_patch.dict(exclude_none=True),
        }
