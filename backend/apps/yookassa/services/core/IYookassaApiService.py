from typing import Optional

from apps.yookassa.models.PaymentModel import PaymentBaseModel, PaymentCreateModel
from apps.yookassa.models.WebhookBaseModel import WebhookBaseModel, WebhookViewModel


class IYookassaApiService:

    async def api_create_payment(
        self,
        cashbox: int,
        warehouse: int,
        payment_crm_id: Optional[int],
        doc_sales_id: Optional[int],
        payment: PaymentCreateModel,
    ):
        raise NotImplementedError

    async def api_create_webhook(
        self, cashbox: int, warehouse: int, webhook: WebhookViewModel
    ):
        raise NotImplementedError

    async def api_get_webhook_list(
        self, cashbox: int, warehouse: int
    ) -> list[WebhookBaseModel]:
        raise NotImplementedError

    async def api_delete_webhook(self, cashbox: int, warehouse: int, webhook_id: str):
        raise NotImplementedError

    async def api_update_payment(self, payment: PaymentBaseModel):
        raise NotImplementedError

    async def api_get_payment_by_docs_sales_id(
        self, docs_sales_id: int
    ) -> Optional[PaymentBaseModel]:
        raise NotImplementedError

    async def api_update_crm_payment_from_webhook_success(self, payment_id: str):
        raise NotImplementedError

    async def api_create_payment_get_good(self, good_id: int):
        raise NotImplementedError
