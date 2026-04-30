from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from fastapi import HTTPException
from functions.helpers import get_user_by_token


class GetWebhookListView:

    def __init__(
        self,
        yookassa_api_service: IYookassaApiService,
    ):
        self.__api_service = yookassa_api_service

    async def __call__(self, token: str, warehouse: int):
        try:
            user = await get_user_by_token(token)
            get_webhook_list = await self.__api_service.api_get_webhook_list(
                user.cashbox_id, warehouse
            )
            return get_webhook_list

        except Exception as error:
            raise HTTPException(
                detail=f"Список webhook не получен: {str(error)}", status_code=432
            )
