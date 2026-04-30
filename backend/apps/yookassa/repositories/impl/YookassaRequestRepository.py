import json
import logging
import uuid
from typing import Optional

import aiohttp
from aiohttp import BasicAuth
from apps.yookassa.models.OauthBaseModel import OauthSettings
from apps.yookassa.models.PaymentModel import PaymentBaseModel, PaymentCreateModel
from apps.yookassa.models.WebhookBaseModel import WebhookBaseModel, WebhookViewModel
from apps.yookassa.repositories.core.IYookassaRequestRepository import (
    IYookassaRequestRepository,
)

logger = logging.getLogger(__name__)


class YookassaRequestRepository(IYookassaRequestRepository):

    async def token(self, code: str, client_id: str, client_secret: str):
        async with aiohttp.ClientSession(
            base_url="https://yookassa.ru", auth=BasicAuth(client_id, client_secret)
        ) as http:
            async with http.post(
                url="/oauth/v2/token",
                data={"grant_type": "authorization_code", "code": code},
            ) as r:
                res = await r.json()
                if res.get("error"):
                    raise Exception(res)
                return res

    async def revoke_token(self, token: str, client_id: str, client_secret: str):
        async with aiohttp.ClientSession(
            base_url="https://yookassa.ru", auth=BasicAuth(client_id, client_secret)
        ) as http:
            async with http.post(
                url="/oauth/v2/revoke_token", data={"token": token}
            ) as r:
                res = await r.json()
                if res.get("error"):
                    raise Exception(res)
                return res

    async def create_webhook(
        self, access_token: str, webhook: WebhookViewModel
    ) -> WebhookBaseModel:
        async with aiohttp.ClientSession(
            base_url="https://api.yookassa.ru",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Idempotence-Key": str(uuid.uuid4()),
            },
        ) as http:
            print(json.dumps(webhook.dict(exclude_none=True)))
            try:
                async with http.post(
                    url="/v3/webhooks", data=json.dumps(webhook.dict(exclude_none=True))
                ) as r:
                    res = await r.json()
                    if res.get("type") == "error":
                        raise Exception(res)
                    return WebhookBaseModel(**res)
            except Exception as error:
                raise Exception(f"ошибка POST запроса к yookassa: {str(error)}")

    async def get_webhook_list(self, access_token: str) -> list[WebhookBaseModel]:
        async with aiohttp.ClientSession(
            base_url="https://api.yookassa.ru",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Idempotence-Key": str(uuid.uuid4()),
            },
        ) as http:

            try:
                async with http.get(url="/v3/webhooks") as r:
                    res = await r.json()
                    if res.get("type") == "error":
                        raise Exception(res)
                    return [WebhookBaseModel(**item) for item in res.get("items")]
            except Exception as error:
                raise Exception(f"ошибка GET запроса к yookassa: {str(error)}")

    async def delete_webhook(self, access_token: str, webhook_id: str):
        async with aiohttp.ClientSession(
            base_url="https://api.yookassa.ru",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Idempotence-Key": str(uuid.uuid4()),
            },
        ) as http:

            try:
                async with http.delete(url=f"/v3/webhooks/{webhook_id}") as r:
                    res = await r.json()
                    if res.get("type") == "error":
                        raise Exception(res)
                    return res
            except Exception as error:
                raise Exception(f"ошибка DELETE запроса к yookassa: {str(error)}")

    async def create_payments(
        self, access_token: str, payment: PaymentCreateModel
    ) -> PaymentBaseModel:
        logger.info(
            f"Запрос к ЮKassa: POST /v3/payments, сумма={payment.amount.value} {payment.amount.currency}"
        )
        logger.debug(f"Тело запроса: {payment.dict(exclude_none=True)}")
        payment_dict = payment.dict(exclude_none=True)
        async with aiohttp.ClientSession(
            base_url="https://api.yookassa.ru",
            headers={
                "Content-Type": "application/json",
                "Idempotence-Key": str(uuid.uuid4()),
                "Authorization": f"Bearer {access_token}",
            },
        ) as http:
            try:
                async with http.post(
                    url="/v3/payments", data=json.dumps(payment_dict)
                ) as r:
                    res = await r.json()
                    if r.status in [400, 401, 403, 404]:
                        logger.error(f"Ошибка ЮKassa: {res}")
                        raise Exception(res)
                    logger.info(
                        f"Ответ ЮKassa: id={res.get('id')}, статус={res.get('status')}"
                    )
                    return PaymentBaseModel(**res)
            except Exception as error:
                logger.exception("Исключение при POST /v3/payments")
                raise Exception(f"ошибка POST запроса к yookassa: {str(error)}")

    async def oauth_settings(self, access_token: str) -> Optional[OauthSettings]:
        async with aiohttp.ClientSession(
            base_url="https://api.yookassa.ru",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
        ) as http:

            try:
                async with http.get(url="/v3/me") as r:
                    res = await r.json()
                    if res.get("type") == "error":
                        raise Exception(res)
                    return OauthSettings(**res)
            except Exception as error:
                raise Exception(f"ошибка /me запроса к yookassa: {str(error)}")
