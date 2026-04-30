from apps.yookassa.models.PaymentModel import PaymentCreateModel, PaymentCreateModelView
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from fastapi import HTTPException
from functions.helpers import get_user_by_token


class CreatePaymentApiView:
    def __init__(self, yookassa_api_service: IYookassaApiService):
        self.__yookassa_api_service = yookassa_api_service

    async def __call__(
        self,
        token: str,
        warehouse: int,
        payment: PaymentCreateModelView,
        payment_crm_id: int = None,
        doc_sales_id: int = None,
    ):
        try:
            payment_subject = {"product": "commodity", "service": "service"}
            user = await get_user_by_token(token)
            for good in payment.receipt.items:
                good.payment_mode = "full_payment"
                good_data = (
                    await self.__yookassa_api_service.api_create_payment_get_good(
                        good.id
                    )
                )
                good_type = (
                    (good_data or {}).get("type")
                    if isinstance(good_data, dict)
                    else None
                )
                good.payment_subject = payment_subject.get(good_type, "commodity")

            payment_yookassa = await self.__yookassa_api_service.api_create_payment(
                cashbox=user.cashbox_id,
                payment_crm_id=payment_crm_id,
                doc_sales_id=doc_sales_id,
                payment=PaymentCreateModel(**payment.dict()),
                warehouse=warehouse,
            )
            return payment_yookassa
        except Exception as error:
            raise HTTPException(
                detail=f"Платеж не создан: {str(error)}", status_code=432
            )
