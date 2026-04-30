from apps.yookassa.models.WebhookBaseModel import WebhookViewModel
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from fastapi import HTTPException
from functions.helpers import get_user_by_token


class CreateWebhookView:

    def __init__(
        self,
        yookassa_api_service: IYookassaApiService,
    ):
        self.__api_service = yookassa_api_service

    async def __call__(self, token: str, warehouse: int, webhook: WebhookViewModel):
        try:
            user = await get_user_by_token(token)
            create_webhook = await self.__api_service.api_create_webhook(
                user.cashbox_id, warehouse, webhook=WebhookViewModel(**webhook.dict())
            )
            return create_webhook

        except Exception as error:
            raise HTTPException(
                detail=f"Webhook не создан: {str(error)}", status_code=432
            )
