from apps.yookassa.services.core.IOauthService import IOauthService
from functions.helpers import get_user_by_token


class GetInstallOauthListView:

    def __init__(
        self,
        oauth_service: IOauthService,
    ):
        self.__oauth_service = oauth_service

    async def __call__(self, token: str):

        user = await get_user_by_token(token)

        install_oauth_list = await self.__oauth_service.get_install_oauth_by_user(
            user.cashbox_id
        )
        return install_oauth_list
