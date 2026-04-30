import base64
import os

from apps.yookassa.services.core.IOauthService import IOauthService
from fastapi import HTTPException
from fastapi.responses import RedirectResponse


class CallbackOauthView:

    def __init__(
        self,
        oauth_service: IOauthService,
    ):
        self.__oauth_service = oauth_service

    async def __call__(self, code: str, state: str):
        try:
            await self.__oauth_service.get_access_token(
                code=code,
                cashbox=int(base64.b64decode(state).decode("utf-8").split(":")[0]),
                warehouse=int(base64.b64decode(state).decode("utf-8").split(":")[1]),
            )
            return RedirectResponse(
                url=f"https://{os.environ.get('APP_URL')}/integrations?token={base64.b64decode(state).decode('utf-8').split(':')[2]}&yookassa=show"
            )
        except Exception as error:
            raise HTTPException(
                status_code=432,
                detail=f"ошибка при авторизации OAuth2 в yookassa.ru: {str(error)}",
            )
