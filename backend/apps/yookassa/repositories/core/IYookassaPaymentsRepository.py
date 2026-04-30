from typing import Optional

from apps.yookassa.models.PaymentModel import PaymentBaseModel


class IYookassaPaymentsRepository:

    async def insert(
        self, oauth_id: int, payment: PaymentBaseModel, payment_crm_id: int
    ):
        raise NotImplementedError

    async def update(
        self, payment: PaymentBaseModel, from_webhook: bool, payment_id_db: str = None
    ):
        raise NotImplementedError

    async def fetch_one(self, payment_id: str) -> Optional[PaymentBaseModel]:
        raise NotImplementedError

    async def fetch_one_by_crm_payment_id(
        self, payment_crm_id: int
    ) -> Optional[PaymentBaseModel]:
        raise NotImplementedError
