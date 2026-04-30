from apps.amocrm.installer.infrastructure.repositories.core.IWidgetInstallerRepository import (
    IWidgetInstallerRepository,
)
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from starlette import status


class GetWidgetInstallerInfoView:

    def __init__(self, widget_installer_repository: IWidgetInstallerRepository):
        self.__widget_installer_repository = widget_installer_repository

    async def __call__(
        self,
        token: str,
        amo_account_id: int,
    ):
        user = await get_user_by_token(token)

        installer_info = await self.__widget_installer_repository.get_installer(
            amo_account_id=amo_account_id, cashbox_id=user.cashbox_id
        )

        if not installer_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "Installer not found"},
            )

        return installer_info
