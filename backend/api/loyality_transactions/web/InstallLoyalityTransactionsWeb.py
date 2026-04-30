from typing import List, Optional, Union

from api.loyality_transactions import schemas
from api.loyality_transactions.web.views.CreateLoyalityTransactionsView import (
    CreateLoyalityTransactionsView,
)
from fastapi import FastAPI
from starlette import status


class InstallLoyalityTransactionsWeb:

    def __call__(self, app: FastAPI):
        create_loyality_transaction_view = CreateLoyalityTransactionsView()

        app.add_api_route(
            path="/loyality_transactions/",
            endpoint=create_loyality_transaction_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["loyality_transactions"],
            response_model=Optional[
                Union[schemas.LoyalityTransaction, List[schemas.LoyalityTransaction]]
            ],
        )
