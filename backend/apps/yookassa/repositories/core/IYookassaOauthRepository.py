from typing import List

from apps.yookassa.models.OauthBaseModel import (
    OauthBaseModel,
    OauthModel,
    OauthUpdateModel,
    OauthWarehouseModel,
)


class IYookassaOauthRepository:

    async def update_oauth(
        self, cashbox: int, warehouse: int, oauth: OauthUpdateModel
    ) -> None:
        raise NotImplementedError

    async def insert_oauth(self, cashbox: int, oauth: OauthModel) -> None:
        raise NotImplementedError

    async def delete_oauth(self, cashbox: int, warehouse: int) -> None:
        raise NotImplementedError

    async def get_oauth(self, cashbox: int, warehouse: int) -> OauthBaseModel:
        raise NotImplementedError

    async def get_oauth_list(self, cashbox: int) -> List[OauthWarehouseModel]:
        raise NotImplementedError
