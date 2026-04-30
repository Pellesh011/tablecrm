from apps.yookassa.models.PaymentModel import (
    PaymentWebhookEventModel,
)
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from fastapi import HTTPException
from fastapi.responses import Response


class EventWebhookView:

    def __init__(
        self,
        yookassa_api_service: IYookassaApiService,
    ):
        self.__api_service = yookassa_api_service

    async def __call__(self, event: PaymentWebhookEventModel):
        try:
            await self.__api_service.api_update_payment(event.object)
            if event.object.status == "succeeded":
                await self.__api_service.api_update_crm_payment_from_webhook_success(
                    event.object.id
                )
            return Response(status_code=200)
        except Exception as error:
            raise HTTPException(
                detail=f"Webhook не обработан: {str(error)}", status_code=432
            )
