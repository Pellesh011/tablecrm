from api.contragents.web.views.CreateContragentsView import CreateContragentsView
from fastapi import FastAPI
from starlette import status


class InstallContragentsWeb:

    def __call__(self, app: FastAPI):
        create_contragents_view = CreateContragentsView()

        app.add_api_route(
            path="/contragents/",
            endpoint=create_contragents_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["contragents"],
        )
