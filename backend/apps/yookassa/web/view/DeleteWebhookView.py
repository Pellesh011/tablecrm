from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from fastapi import HTTPException
from functions.helpers import get_user_by_token


class DeleteWebhookView:

    def __init__(
        self,
        yookassa_api_service: IYookassaApiService,
    ):
        self.__api_service = yookassa_api_service

    async def __call__(self, token: str, warehouse: int, webhook_id: str):
        try:
            user = await get_user_by_token(token)
            delete_webhook = await self.__api_service.api_delete_webhook(
                user.cashbox_id, warehouse, webhook_id
            )
            return delete_webhook

        except Exception as error:
            raise HTTPException(
                detail=f"Webhook не удален: {str(error)}", status_code=432
            )
