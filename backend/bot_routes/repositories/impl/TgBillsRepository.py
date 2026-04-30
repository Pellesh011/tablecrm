from typing import Union

from bot_routes.models.TgBillsModels import (
    TgBillsCreateModel,
    TgBillsExtendedModel,
    TgBillsUpdateModel,
)
from bot_routes.repositories.core.ITgBillsRepository import ITgBillsRepository
from database.db import tg_bot_bills
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError


class TgBillsRepository(ITgBillsRepository):

    def __init__(self, database, tg_bot_bills, tochka_bank_accounts):
        self.database = database
        self.tg_bot_bills = tg_bot_bills
        self.tochka_bank_accounts = tochka_bank_accounts

    async def update(self, id, bill: TgBillsUpdateModel) -> None:
        try:
            query = (
                self.tg_bot_bills.update()
                .where(self.tg_bot_bills.c.id == id)
                .values(**bill.dict(exclude_unset=True))
            )
            await self.database.execute(query)
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def insert(self, bill: TgBillsCreateModel) -> int:
        try:
            query = self.tg_bot_bills.insert().values(**bill.dict())
            result = await self.database.execute(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def delete(self, id: int) -> None:
        try:
            query = self.tg_bot_bills.delete().where(self.tg_bot_bills.c.id == id)
            await self.atabase.execute(query)
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_by_id(self, id: int) -> Union[TgBillsExtendedModel, None]:
        try:
            query = (
                select([self.tg_bot_bills, self.tochka_bank_accounts.c.accountId])
                .select_from(tg_bot_bills)
                .outerjoin(
                    self.tochka_bank_accounts,
                    self.tg_bot_bills.c.tochka_bank_account_id
                    == self.tochka_bank_accounts.c.id,
                )
                .where(self.tg_bot_bills.c.id == id)
            )
            result = await self.database.fetch_one(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")
