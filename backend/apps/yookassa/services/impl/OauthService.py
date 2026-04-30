import base64
import os

from apps.yookassa.functions.core.IGetOauthCredentialFunction import (
    IGetOauthCredentialFunction,
)
from apps.yookassa.models.OauthBaseModel import (
    OauthBaseModel,
    OauthUpdateModel,
)
from apps.yookassa.models.WebhookBaseModel import WebhookViewModel
from apps.yookassa.repositories.core.IYookassaOauthRepository import (
    IYookassaOauthRepository,
)
from apps.yookassa.repositories.core.IYookassaRequestRepository import (
    IYookassaRequestRepository,
)
from apps.yookassa.services.core.IOauthService import IOauthService


class OauthService(IOauthService):

    def __init__(
        self,
        oauth_repository: IYookassaOauthRepository,
        request_repository: IYookassaRequestRepository,
        get_oauth_credential_function: IGetOauthCredentialFunction,
    ):

        self.__oauth_repository = oauth_repository
        self.__request_repository = request_repository
        self.__get_oauth_credential_function = get_oauth_credential_function

    async def oauth_link(self, cashbox: int, warehouse: int, token: str) -> str:

        client_id, _ = self.__get_oauth_credential_function()
        return f'https://yookassa.ru/oauth/v2/authorize?client_id={client_id}&response_type=code&state={base64.b64encode(f"{cashbox}:{warehouse}:{token}".encode("utf-8")).decode("utf-8")}'

    async def revoke_token(self, cashbox: int, warehouse: int):
        try:

            client_id, client_secret = self.__get_oauth_credential_function()
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)

            webhook_list = await self.__request_repository.get_webhook_list(
                access_token=oauth.access_token
            )

            for webhook in webhook_list:
                await self.__request_repository.delete_webhook(
                    access_token=oauth.access_token, webhook_id=webhook.id
                )

            if not oauth:
                raise Exception("Отсутствует oauth2 по данному пользователю")

            await self.__request_repository.revoke_token(
                token=oauth.access_token,
                client_id=client_id,
                client_secret=client_secret,
            )
            await self.__oauth_repository.delete_oauth(
                cashbox=cashbox, warehouse=warehouse
            )

        except Exception as error:
            raise error

    async def get_access_token(self, code: str, cashbox: int, warehouse: int):

        client_id, client_secret = self.__get_oauth_credential_function()

        try:
            res = await self.__request_repository.token(
                code=code, client_id=client_id, client_secret=client_secret
            )
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)

            if oauth:
                await self.__oauth_repository.update_oauth(
                    cashbox,
                    warehouse,
                    OauthUpdateModel(
                        access_token=res.get("access_token"),
                        warehouse_id=warehouse,
                        is_deleted=False,
                    ),
                )
            else:
                await self.__oauth_repository.insert_oauth(
                    cashbox,
                    OauthBaseModel(
                        cashbox_id=cashbox,
                        access_token=res.get("access_token"),
                        warehouse_id=warehouse,
                    ),
                )

            await self.__request_repository.create_webhook(
                access_token=res.get("access_token"),
                webhook=WebhookViewModel(
                    event="payment.waiting_for_capture",
                    url=f"https://{os.environ.get('APP_URL')}/api/v1/yookassa/webhook/event",
                ),
            )

            await self.__request_repository.create_webhook(
                access_token=res.get("access_token"),
                webhook=WebhookViewModel(
                    event="payment.succeeded",
                    url=f"https://{os.environ.get('APP_URL')}/api/v1/yookassa/webhook/event",
                ),
            )

            await self.__request_repository.create_webhook(
                access_token=res.get("access_token"),
                webhook=WebhookViewModel(
                    event="payment.canceled",
                    url=f"https://{os.environ.get('APP_URL')}/api/v1/yookassa/webhook/event",
                ),
            )

        except Exception as error:
            raise error

    async def get_install_oauth_by_user(self, cashbox: int):

        install_oauth_list = await self.__oauth_repository.get_oauth_list(cashbox)

        return install_oauth_list

    async def validation_oauth(self, cashbox: int, warehouse: int):
        try:
            oauth = await self.__oauth_repository.get_oauth(cashbox, warehouse)
            settings = await self.__request_repository.oauth_settings(
                oauth.access_token
            )
            if settings:
                return True
        except Exception as error:
            return False
