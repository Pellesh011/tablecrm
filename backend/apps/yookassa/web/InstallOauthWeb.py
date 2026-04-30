from apps.yookassa.functions.impl.GetOauthCredentialFunction import (
    GetOauthCredentialFunction,
)
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
from apps.yookassa.services.impl.OauthService import OauthService
from apps.yookassa.services.impl.YookassaApiService import YookassaApiService
from apps.yookassa.web.view.CallbackOauthView import CallbackOauthView
from apps.yookassa.web.view.CreateOauthView import CreateOauthView
from apps.yookassa.web.view.CreatePaymentApiView import CreatePaymentApiView
from apps.yookassa.web.view.CreateWebhookView import CreateWebhookView
from apps.yookassa.web.view.DeleteWebhookView import DeleteWebhookView
from apps.yookassa.web.view.EventWebhookView import EventWebhookView
from apps.yookassa.web.view.GetInstallOauthListView import GetInstallOauthListView
from apps.yookassa.web.view.GetMeApiView import GetMeApiView
from apps.yookassa.web.view.GetPaymentApiView import GetPaymentApiView
from apps.yookassa.web.view.GetWebhookListView import GetWebhookListView
from apps.yookassa.web.view.RevokeTokenOauthView import RevokeTokenOauthView
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI, status


class InstallYookassaOauthWeb:

    def __call__(self, app: FastAPI):
        create_oauth_view = CreateOauthView(
            oauth_service=OauthService(
                oauth_repository=ioc.get(IYookassaOauthRepository),
                request_repository=ioc.get(IYookassaRequestRepository),
                get_oauth_credential_function=GetOauthCredentialFunction(),
            )
        )

        callback_oauth_view = CallbackOauthView(
            oauth_service=OauthService(
                oauth_repository=ioc.get(IYookassaOauthRepository),
                request_repository=ioc.get(IYookassaRequestRepository),
                get_oauth_credential_function=GetOauthCredentialFunction(),
            )
        )

        revoke_token_oauth_view = RevokeTokenOauthView(
            oauth_service=OauthService(
                oauth_repository=ioc.get(IYookassaOauthRepository),
                request_repository=ioc.get(IYookassaRequestRepository),
                get_oauth_credential_function=GetOauthCredentialFunction(),
            )
        )

        create_payment_api_view = CreatePaymentApiView(
            yookassa_api_service=YookassaApiService(
                request_repository=ioc.get(IYookassaRequestRepository),
                oauth_repository=ioc.get(IYookassaOauthRepository),
                payments_repository=ioc.get(IYookassaPaymentsRepository),
                crm_payments_repository=ioc.get(IYookassaCrmPaymentsRepository),
                amo_table_crm_repository=ioc.get(IYookasssaAmoTableCrmRepository),
                table_nomenclature_repository=ioc.get(IYookassaTableNomenclature),
            )
        )

        get_install_oauth_list = GetInstallOauthListView(
            oauth_service=OauthService(
                oauth_repository=ioc.get(IYookassaOauthRepository),
                request_repository=ioc.get(IYookassaRequestRepository),
                get_oauth_credential_function=GetOauthCredentialFunction(),
            )
        )

        create_webhook = CreateWebhookView(
            yookassa_api_service=YookassaApiService(
                request_repository=ioc.get(IYookassaRequestRepository),
                oauth_repository=ioc.get(IYookassaOauthRepository),
                payments_repository=ioc.get(IYookassaPaymentsRepository),
                crm_payments_repository=ioc.get(IYookassaCrmPaymentsRepository),
                amo_table_crm_repository=ioc.get(IYookasssaAmoTableCrmRepository),
                table_nomenclature_repository=ioc.get(IYookassaTableNomenclature),
            )
        )

        get_webhook_list = GetWebhookListView(
            yookassa_api_service=YookassaApiService(
                request_repository=ioc.get(IYookassaRequestRepository),
                oauth_repository=ioc.get(IYookassaOauthRepository),
                payments_repository=ioc.get(IYookassaPaymentsRepository),
                crm_payments_repository=ioc.get(IYookassaCrmPaymentsRepository),
                amo_table_crm_repository=ioc.get(IYookasssaAmoTableCrmRepository),
                table_nomenclature_repository=ioc.get(IYookassaTableNomenclature),
            )
        )

        delete_webhook = DeleteWebhookView(
            yookassa_api_service=YookassaApiService(
                request_repository=ioc.get(IYookassaRequestRepository),
                oauth_repository=ioc.get(IYookassaOauthRepository),
                payments_repository=ioc.get(IYookassaPaymentsRepository),
                crm_payments_repository=ioc.get(IYookassaCrmPaymentsRepository),
                amo_table_crm_repository=ioc.get(IYookasssaAmoTableCrmRepository),
                table_nomenclature_repository=ioc.get(IYookassaTableNomenclature),
            )
        )

        event_webhook = EventWebhookView(
            yookassa_api_service=YookassaApiService(
                request_repository=ioc.get(IYookassaRequestRepository),
                oauth_repository=ioc.get(IYookassaOauthRepository),
                payments_repository=ioc.get(IYookassaPaymentsRepository),
                crm_payments_repository=ioc.get(IYookassaCrmPaymentsRepository),
                amo_table_crm_repository=ioc.get(IYookasssaAmoTableCrmRepository),
                table_nomenclature_repository=ioc.get(IYookassaTableNomenclature),
            )
        )

        get_payment_api_view = GetPaymentApiView(
            yookassa_api_service=YookassaApiService(
                request_repository=ioc.get(IYookassaRequestRepository),
                oauth_repository=ioc.get(IYookassaOauthRepository),
                payments_repository=ioc.get(IYookassaPaymentsRepository),
                crm_payments_repository=ioc.get(IYookassaCrmPaymentsRepository),
                amo_table_crm_repository=ioc.get(IYookasssaAmoTableCrmRepository),
                table_nomenclature_repository=ioc.get(IYookassaTableNomenclature),
            )
        )

        get_me_api_view = GetMeApiView(
            oauth_service=OauthService(
                oauth_repository=ioc.get(IYookassaOauthRepository),
                request_repository=ioc.get(IYookassaRequestRepository),
                get_oauth_credential_function=GetOauthCredentialFunction(),
            )
        )

        app.add_api_route(
            path="/yookassa/me",
            endpoint=get_me_api_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/payments/from_sales",
            endpoint=get_payment_api_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/webhook/event",
            endpoint=event_webhook.__call__,
            methods=["POST"],
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/webhook/delete",
            endpoint=delete_webhook.__call__,
            methods=["DELETE"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/webhook/list",
            endpoint=get_webhook_list.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/webhook/create",
            endpoint=create_webhook.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/install/list",
            endpoint=get_install_oauth_list.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/events",
            endpoint=callback_oauth_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/revoke",
            endpoint=revoke_token_oauth_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/payments",
            endpoint=create_payment_api_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
            tags=["yookassa"],
        )

        app.add_api_route(
            path="/yookassa/install",
            endpoint=create_oauth_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            tags=["yookassa"],
        )
