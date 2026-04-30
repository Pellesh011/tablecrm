from apps.yookassa.models.PaymentModel import EventWebhookPayment
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from fastapi import HTTPException
from functions.helpers import get_user_by_token


class GetPaymentApiView:

    def __init__(self, yookassa_api_service: IYookassaApiService):
        self.__yookassa_api_service = yookassa_api_service

    async def __call__(self, token: str, doc_sales_id: int):
        try:
            user = await get_user_by_token(token)

            payment_yookassa = (
                await self.__yookassa_api_service.api_get_payment_by_docs_sales_id(
                    docs_sales_id=doc_sales_id,
                )
            )
            if payment_yookassa:
                if payment_yookassa.status == EventWebhookPayment.pending:
                    return payment_yookassa.dict(exclude_unset=True)
                else:
                    payment = payment_yookassa.dict(exclude_unset=True)
                    del payment["confirmation"]
                    return payment
        except Exception as error:
            raise HTTPException(
                detail=f"Ошибка получения платежа: {str(error)}", status_code=432
            )
