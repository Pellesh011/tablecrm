import logging
import os
import uuid
from typing import Optional

from apps.yookassa.models.PaymentModel import (
    EventWebhookPayment,
    PaymentBaseModel,
    PaymentCreateModel,
)
from apps.yookassa.models.WebhookBaseModel import WebhookBaseModel, WebhookViewModel
from apps.yookassa.models.YookassaLinkPushMessage import YookassaLinkPushMessage
from apps.yookassa.repositories.core.IYookassaCrmPaymentsRepository import (
    IYookassaCrmPaymentsRepository,
)
from apps.yookassa.repositories.core.IYookassaOauthRepository import (
    IYookassaOauthRepository,
)
from apps.yookassa.repositories.core.IYookassaPaymentsRepository import (
    IYookassaPaymentsRepository,
)
from apps.yookassa.repositories.core.IYookassaRequestRepository import (
    IYookassaRequestRepository,
)
from apps.yookassa.repositories.core.IYookassaTableNomenclature import (
    IYookassaTableNomenclature,
)
from apps.yookassa.repositories.core.IYookasssaAmoTableCrmRepository import (
    IYookasssaAmoTableCrmRepository,
)
from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService
from common.amqp_messaging.common.impl.RabbitFactory import RabbitFactory
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings

logger = logging.getLogger(__name__)


class YookassaApiService(IYookassaApiService):
    def __init__(
        self,
        request_repository: IYookassaRequestRepository,
        oauth_repository: IYookassaOauthRepository,
        payments_repository: IYookassaPaymentsRepository,
        crm_payments_repository: IYookassaCrmPaymentsRepository,
        amo_table_crm_repository: IYookasssaAmoTableCrmRepository,
        table_nomenclature_repository: IYookassaTableNomenclature,
    ):
        self.__request_repository = request_repository
        self.__oauth_repository = oauth_repository
        self.__payments_repository = payments_repository
        self.__crm_payments_repository = crm_payments_repository
        self.__amo_table_crm_repository = amo_table_crm_repository
        self.__table_nomenclature_repository = table_nomenclature_repository

    async def api_create_webhook(
        self, cashbox: int, warehouse: int, webhook: WebhookViewModel
    ):
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            response = await self.__request_repository.create_webhook(
                access_token=oauth.access_token, webhook=webhook
            )
            return response
        except Exception as error:
            raise Exception(f"ошибка создания webhook: {str(error)}")

    async def api_get_webhook_list(
        self, cashbox: int, warehouse: int
    ) -> list[WebhookBaseModel]:
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            response = await self.__request_repository.get_webhook_list(
                access_token=oauth.access_token
            )
            return response
        except Exception as error:
            raise Exception(f"ошибка получения списка webhook: {str(error)}")

    async def api_delete_webhook(self, cashbox: int, warehouse: int, webhook_id: str):
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            response = await self.__request_repository.delete_webhook(
                access_token=oauth.access_token, webhook_id=webhook_id
            )
            return response
        except Exception as error:
            raise Exception(f"ошибка удаления webhook: {str(error)}")

    async def api_create_payment(
        self,
        cashbox: int,
        warehouse: int,
        doc_sales_id: Optional[int],
        payment_crm_id: Optional[int],
        payment: PaymentCreateModel,
    ):
        logger.info(
            f"api_create_payment: cashbox={cashbox}, warehouse={warehouse}, "
            f"doc_sales_id={doc_sales_id}, payment_crm_id={payment_crm_id}"
        )
        logger.debug(
            f"Параметры платежа: сумма={payment.amount.value}, capture={payment.capture}"
        )

        print(payment, payment_crm_id)
        if doc_sales_id and (payment_crm_id is None):
            logger.debug(f"Ищем crm-платеж по doc_sales_id={doc_sales_id}")
            crm_payment = (
                await self.__crm_payments_repository.get_crm_payments_by_doc_sales_id(
                    doc_sales_id
                )
            )
            if crm_payment is None:
                raise Exception(
                    f"по документу продажи {doc_sales_id} не найден платёж (payments). "
                    "Укажите существующий doc_sales_id или payment_crm_id."
                )
            logger.info(f"Найден crm-платеж id={payment_crm_id}")
            payment_crm_id = crm_payment.id
        try:
            print(0)
            logger.debug(f"Получаем OAuth для cashbox={cashbox}, warehouse={warehouse}")
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            if not oauth:
                raise Exception("для склада продажи не установлена интеграция с Юkassa")
            payment_db = await self.__payments_repository.fetch_one_by_crm_payment_id(
                payment_crm_id
            )
            print(1)
            logger.info(f"OAuth получен (токен: {oauth.access_token[:8]}...)")
            settings = await self.__request_repository.oauth_settings(
                access_token=oauth.access_token
            )
            print(2)
            if settings:
                payment.test = settings.test
                if settings.fiscalization and not settings.fiscalization.enabled:
                    # Не удаляем чек, если фронт прислал receipt с позициями (в т.ч. vat_code)
                    if not (
                        payment.receipt and getattr(payment.receipt, "items", None)
                    ):
                        del payment.receipt
            print(3)
            response = await self.__request_repository.create_payments(
                access_token=oauth.access_token, payment=payment
            )
            print(4)
            logger.debug("Отправляем событие в RabbitMQ")
            """ отправка в очередь """
            rabbit_factory = RabbitFactory(
                settings=RabbitMqSettings(
                    rabbitmq_host=os.getenv("RABBITMQ_HOST_AMO_INTEGRATION"),
                    rabbitmq_user=os.getenv("RABBITMQ_USER_AMO_INTEGRATION"),
                    rabbitmq_pass=os.getenv("RABBITMQ_PASS_AMO_INTEGRATION"),
                    rabbitmq_port=os.getenv("RABBITMQ_PORT_AMO_INTEGRATION"),
                    rabbitmq_vhost=os.getenv("RABBITMQ_VHOST_AMO_INTEGRATION"),
                )
            )
            factory = await rabbit_factory()
            amqp_messaging = await factory()

            install = (
                await self.__amo_table_crm_repository.get_active_install_by_cashbox(
                    cashbox
                )
            )
            print(5)
            print(6)
            if install is not None:
                await amqp_messaging.publish(
                    YookassaLinkPushMessage(
                        **dict(install),
                        docs_sales_id=doc_sales_id,
                        message_id=uuid.uuid4(),
                        payment=response.dict(exclude_none=True),
                    ),
                    f"amo.{dict(install).get('referrer')}.push.yookassa.link",
                )

            """ окончание отправки """

            if not payment_db:
                await self.__payments_repository.insert(
                    oauth_id=oauth.id,
                    payment=PaymentBaseModel(
                        **response.dict(exclude_none=True), capture=payment.capture
                    ),
                    payment_crm_id=payment_crm_id,
                )
                logger.info(f"Платеж сохранён в БД, payment_id={response.id}")
                return response
            elif payment_db.status == EventWebhookPayment.pending:
                await self.__payments_repository.update(
                    PaymentBaseModel(
                        **response.dict(exclude_none=True), capture=payment.capture
                    ),
                    from_webhook=False,
                    payment_id_db=payment_db.id,
                )
                logger.info(f"Платеж обновлён в БД, payment_id={response.id}")
                return response
            elif payment_db.status == EventWebhookPayment.waiting_for_capture:
                raise Exception("платеж yookassa ожидает подтверждения")
            else:
                raise Exception("платеж yookassa подтвержден и его нельзя изменить")

        except Exception as e:
            logger.exception(f"Ошибка в api_create_payment: {e}")
            raise Exception(f"ошибка создания платежа: {str(e)}")

    async def api_update_payment(self, payment: PaymentBaseModel):
        try:
            return await self.__payments_repository.update(payment, from_webhook=True)
        except Exception as error:
            raise Exception(f"ошибка обновления платежа: {str(error)}")

    async def api_get_payment_by_docs_sales_id(
        self, docs_sales_id: int
    ) -> Optional[PaymentBaseModel]:
        try:
            crm_payment = (
                await self.__crm_payments_repository.get_crm_payments_by_doc_sales_id(
                    docs_sales_id
                )
            )
            if not crm_payment:
                raise Exception("платеж не найден по документу продажи")
            return await self.__payments_repository.fetch_one_by_crm_payment_id(
                crm_payment.id
            )
        except Exception as error:
            raise Exception(f"ошибка обновления платежа: {str(error)}")

    async def api_update_crm_payment_from_webhook_success(self, payment_id: str):
        try:
            payment_db = await self.__payments_repository.fetch_one(payment_id)
            await self.__crm_payments_repository.update_crm_payments_by_webhook_success(
                payment_db.payment_crm_id
            )
        except Exception as error:
            raise Exception(f"ошибка обновления платежа crm: {str(error)}")

    async def api_create_payment_get_good(self, good_id: int):
        try:
            return await self.__table_nomenclature_repository.fetch_one_by_id(good_id)
        except Exception as error:
            raise Exception(f"ошибка получения данных о товарах платежа: {str(error)}")
