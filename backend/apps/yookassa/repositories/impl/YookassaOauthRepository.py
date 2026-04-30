from typing import List, Union

from apps.yookassa.models.OauthBaseModel import (
    OauthBaseModel,
    OauthUpdateModel,
    OauthWarehouseModel,
)
from apps.yookassa.repositories.core.IYookassaOauthRepository import (
    IYookassaOauthRepository,
)
from database.db import database, warehouses, yookassa_install
from sqlalchemy import insert, select, update


class YookassaOauthRepository(IYookassaOauthRepository):

    async def get_oauth(
        self, cashbox: int, warehouse: int
    ) -> Union[OauthBaseModel, None]:
        try:
            query = select(yookassa_install).where(
                yookassa_install.c.cashbox_id == cashbox,
                yookassa_install.c.warehouse_id == warehouse,
            )
            oauth = await database.fetch_one(query)
            if oauth:
                return OauthBaseModel(**oauth)
            else:
                return None
        except Exception as error:
            raise Exception(f"ошибка БД: {str(error)}")

    async def update_oauth(self, cashbox: int, warehouse: int, oauth: OauthUpdateModel):
        oauth_db_model = await self.get_oauth(cashbox, warehouse)
        update_data = oauth.dict(exclude_none=True)
        update_oauth = oauth_db_model.copy(update=update_data)
        query = (
            update(yookassa_install)
            .where(
                yookassa_install.c.cashbox_id == cashbox,
                yookassa_install.c.warehouse_id == warehouse,
            )
            .values(update_oauth.dict())
            .returning(yookassa_install.c.id)
        )

        return await database.execute(query)

    async def insert_oauth(self, cashbox: int, oauth: OauthBaseModel):
        query = (
            insert(yookassa_install)
            .values(oauth.dict(exclude_none=True))
            .returning(yookassa_install.c.id)
        )
        return await database.execute(query)

    async def delete_oauth(self, cashbox: int, warehouse: int):
        oauth = await self.get_oauth(cashbox, warehouse)
        await self.update_oauth(cashbox, warehouse, OauthUpdateModel(is_deleted=True))

    async def get_oauth_list(
        self, cashbox: int
    ) -> Union[List[OauthWarehouseModel], None]:
        try:
            query = (
                select(
                    yookassa_install,
                    warehouses.c.name,
                    warehouses.c.description,
                    warehouses.c.id,
                )
                .where(yookassa_install.c.cashbox_id == cashbox)
                .select_from(yookassa_install)
                .join(warehouses, yookassa_install.c.warehouse_id == warehouses.c.id)
            )
            oauth_list = await database.fetch_all(query)
            if oauth_list:
                return [
                    OauthWarehouseModel(
                        cashbox=oauth.get("cashbox_id"),
                        warehouse_name=oauth.get("name"),
                        warehouse_id=oauth.get("warehouse_id"),
                        warehouse_description=oauth.get("description"),
                        last_update=oauth.get("updated_at"),
                        status=True if not oauth.get("is_deleted") else False,
                    )
                    for oauth in oauth_list
                ]
            else:
                return []
        except Exception as error:
            raise Exception(f"ошибка БД: {str( error )}")
