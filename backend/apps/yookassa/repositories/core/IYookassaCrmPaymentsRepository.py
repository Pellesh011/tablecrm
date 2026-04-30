class IYookassaCrmPaymentsRepository:

    async def get_crm_payments_by_doc_sales_id(self, doc_sales_id: int):
        raise NotImplementedError

    async def update_crm_payments_by_webhook_success(self, payment_id: int):
        raise NotImplementedError
