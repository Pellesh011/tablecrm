from apps.yookassa.services.core.IOauthService import IOauthService
from fastapi import HTTPException
from functions.helpers import get_user_by_token


class RevokeTokenOauthView:

    def __init__(
        self,
        oauth_service: IOauthService,
    ):
        self.__oauth_service = oauth_service

    async def __call__(self, token: str, warehouse: int):
        user = await get_user_by_token(token)
        try:
            res = await self.__oauth_service.revoke_token(user.cashbox_id, warehouse)
            return res
        except Exception as error:
            raise HTTPException(
                status_code=432,
                detail=f"ошибка при отзыве access_token в yookassa.ru: {str(error)}",
            )
