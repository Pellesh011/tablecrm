from typing import Optional, Union

from apps.yookassa.models.OauthBaseModel import OauthSettings
from apps.yookassa.models.PaymentModel import PaymentBaseModel, PaymentCreateModel
from apps.yookassa.models.WebhookBaseModel import WebhookBaseModel, WebhookViewModel


class IYookassaRequestRepository:

    async def token(self, code: str, client_id: str, client_secret: str):
        raise NotImplementedError

    async def revoke_token(
        self, token: str, client_id: str, client_secret: str
    ) -> None:
        raise NotImplementedError

    async def create_payments(
        self, access_token: str, payment: PaymentCreateModel
    ) -> Union[PaymentBaseModel]:
        raise NotImplementedError

    async def create_webhook(
        self, access_token, webhook: WebhookViewModel
    ) -> WebhookBaseModel:
        raise NotImplementedError

    async def get_webhook_list(self, access_token: str) -> list[WebhookBaseModel]:
        raise NotImplementedError

    async def delete_webhook(self, access_token: str, webhook_id: str):
        raise NotImplementedError

    async def oauth_settings(self, access_token: str) -> Optional[OauthSettings]:
        raise NotImplementedError
