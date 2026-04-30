from api.docs_sales.web.views.CreateDocsSalesView import CreateDocsSalesView
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc
from fastapi import FastAPI
from starlette import status


class InstallDocsSalesWeb:

    def __call__(self, app: FastAPI):
        create_docs_sales_view = CreateDocsSalesView(
            rabbitmq_messaging_factory=ioc.get(IRabbitFactory)
        )

        app.add_api_route(
            path="/docs_sales/",
            endpoint=create_docs_sales_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["docs_sales"],
        )
