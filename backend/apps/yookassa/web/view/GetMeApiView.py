from apps.yookassa.services.core.IOauthService import IOauthService
from functions.helpers import get_user_by_token


class GetMeApiView:

    def __init__(
        self,
        oauth_service: IOauthService,
    ):
        self.__oauth_service = oauth_service

    async def __call__(self, token: str, warehouse: int):

        user = await get_user_by_token(token)

        me = await self.__oauth_service.validation_oauth(user.cashbox_id, warehouse)
        return me
