from apps.yookassa.services.core.IOauthService import IOauthService
from functions.helpers import get_user_by_token


class CreateOauthView:

    def __init__(
        self,
        oauth_service: IOauthService,
    ):
        self.__oauth_service = oauth_service

    async def __call__(self, token: str, warehouse: int):
        user = await get_user_by_token(token)
        create_link = await self.__oauth_service.oauth_link(
            user.cashbox_id, warehouse, token
        )
        return create_link
