import logging
from typing import Optional

from apps.yookassa.models.PaymentModel import (
    AmountModel,
    ConfirmationRedirectResponce,
    PaymentBaseModel,
)
from apps.yookassa.repositories.core.IYookassaPaymentsRepository import (
    IYookassaPaymentsRepository,
)
from database.db import database, yookassa_payments
from sqlalchemy import insert, select, update

logger = logging.getLogger(__name__)


class YookassaPaymentsRepository(IYookassaPaymentsRepository):

    async def insert(
        self, oauth_id: int, payment: PaymentBaseModel, payment_crm_id: int
    ):
        logger.debug(
            f"Вставка платежа в БД: payment_id={payment.id}, payment_crm_id={payment_crm_id}"
        )
        query = (
            insert(yookassa_payments)
            .values(
                {
                    "payment_crm_id": payment_crm_id,
                    "payment_id": payment.id,
                    "status": payment.status,
                    "amount_value": float(payment.amount.value),
                    "amount_currency": payment.amount.currency,
                    "income_amount_value": (
                        float(payment.income_amount.value)
                        if payment.income_amount
                        else None
                    ),
                    "income_amount_currency": (
                        payment.income_amount.currency
                        if payment.income_amount
                        else None
                    ),
                    "description": payment.description,
                    "is_deleted": False,
                    "confirmation_url": payment.confirmation.confirmation_url,
                    "payment_capture": payment.capture,
                }
            )
            .returning(yookassa_payments.c.id)
        )
        result = await database.execute(query)
        logger.info(f"Платеж сохранён, id записи={result}")
        return await database.execute(query)

    async def fetch_one(self, payment_id: str) -> Optional[PaymentBaseModel]:
        payment_db = await database.fetch_one(
            select(yookassa_payments).where(
                yookassa_payments.c.payment_id == payment_id
            )
        )
        logger.debug(f"Поиск платежа по payment_id={payment_id}")
        if payment_db:
            logger.debug(f"Платеж найден: статус={payment_db.status}")
            return PaymentBaseModel(
                id=payment_db.payment_id,
                status=payment_db.status,
                amount=AmountModel(
                    value=payment_db.amount_value,
                    currency=payment_db.amount_currency,
                ),
                income_amount=AmountModel(
                    value=payment_db.income_amount_value,
                    currency=payment_db.income_amount_currency,
                ),
                capture=payment_db.payment_capture,
                created_at=payment_db.updated_at,
                payment_crm_id=payment_db.payment_crm_id,
                confirmation=ConfirmationRedirectResponce(
                    confirmation_url=payment_db.confirmation_url
                ),
            )
        else:
            logger.debug(f"Платеж с payment_id={payment_id} не найден")
            return None

    async def fetch_one_by_crm_payment_id(
        self, payment_crm_id: int
    ) -> Optional[PaymentBaseModel]:
        payment_db = await database.fetch_one(
            select(yookassa_payments).where(
                yookassa_payments.c.payment_crm_id == payment_crm_id
            )
        )
        if payment_db:
            return PaymentBaseModel(
                id=payment_db.payment_id,
                status=payment_db.status,
                amount=AmountModel(
                    value=payment_db.amount_value,
                    currency=payment_db.amount_currency,
                ),
                created_at=payment_db.updated_at,
                capture=payment_db.payment_capture,
                confirmation=ConfirmationRedirectResponce(
                    confirmation_url=payment_db.confirmation_url, type="redirect"
                ),
            )
        else:
            return None

    async def update(
        self, payment: PaymentBaseModel, from_webhook: bool, payment_id_db: str = None
    ):
        logger.debug(
            f"Обновление платежа в БД: payment_id={payment.id}, from_webhook={from_webhook}"
        )
        if from_webhook:
            data = {
                "status": payment.status,
                "income_amount_value": (
                    float(payment.income_amount.value)
                    if payment.income_amount
                    else None
                ),
                "income_amount_currency": (
                    payment.income_amount.currency if payment.income_amount else None
                ),
            }
        else:
            data = {
                "payment_id": payment.id,
                "amount_value": float(payment.amount.value),
                "amount_currency": payment.amount.currency,
                "confirmation_url": payment.confirmation.confirmation_url,
                "payment_capture": payment.capture,
            }
        payment_query = payment.id if payment_id_db is None else payment_id_db
        query = (
            update(yookassa_payments)
            .where(yookassa_payments.c.payment_id == payment_query)
            .values(data)
            .returning(yookassa_payments.c.id)
        )
        result = await database.execute(query)
        logger.info(f"Платеж обновлён, id записи={result}")
        return await database.execute(query)
