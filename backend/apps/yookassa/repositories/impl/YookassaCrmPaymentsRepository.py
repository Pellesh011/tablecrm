from apps.yookassa.repositories.core.IYookassaCrmPaymentsRepository import (
    IYookassaCrmPaymentsRepository,
)
from database.db import database, docs_sales, payments
from sqlalchemy import select, update


class YookassaCrmPaymentsRepository(IYookassaCrmPaymentsRepository):

    async def get_crm_payments_by_doc_sales_id(self, doc_sales_id: int):
        return await database.fetch_one(
            select(payments, docs_sales.c.warehouse)
            .select_from(payments)
            .join(docs_sales, docs_sales.c.id == payments.c.docs_sales_id)
            .where(payments.c.docs_sales_id == doc_sales_id)
        )

    async def update_crm_payments_by_webhook_success(self, payment_id: int):
        return await database.execute(
            update(payments).where(payments.c.id == payment_id).values({"status": True})
        )
