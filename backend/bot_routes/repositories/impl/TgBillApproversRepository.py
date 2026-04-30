from typing import List, Union

from bot_routes.models.TgBillApproversModels import (
    TgBillApproversCreateModel,
    TgBillApproversExtendedModel,
    TgBillApproversModel,
    TgBillApproversUpdateModel,
)
from bot_routes.repositories.core.ITgBillApproversRepository import (
    ITgBillApproversRepository,
)
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError


class TgBillApproversRepository(ITgBillApproversRepository):
    def __init__(self, database, tg_bot_bill_approvers, users):
        self.database = database
        self.users = users
        self.tg_bot_bill_approvers = tg_bot_bill_approvers

    async def update(self, id: int, bill: TgBillApproversUpdateModel) -> None:
        try:
            query = (
                self.tg_bot_bill_approvers.update()
                .where(self.tg_bot_bill_approvers.c.id == id)
                .values(**bill.dict(exclude_unset=True))
            )
            await self.database.execute(query)
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def insert(self, bill: TgBillApproversCreateModel) -> int:
        try:
            query = self.tg_bot_bill_approvers.insert().values(**bill.dict())
            result = await self.database.execute(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def delete(self, id: int) -> None:
        try:
            query = self.tg_bot_bill_approvers.delete().where(
                self.tg_bot_bill_approvers.c.id == id
            )
            await self.database.execute(query)
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_by_id(self, id: int) -> Union[TgBillApproversModel, None]:
        try:
            query = self.tg_bot_bill_approvers.select().where(
                self.tg_bot_bill_approvers.c.id == id
            )
            result = await self.database.fetch_one(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_approve_by_bill_id_and_approver_id(
        self, bill_id: int, approver_id: int
    ) -> Union[TgBillApproversModel, None]:
        try:
            query = self.tg_bot_bill_approvers.select().where(
                self.tg_bot_bill_approvers.c.bill_id == bill_id,
                self.tg_bot_bill_approvers.c.approver_id == approver_id,
            )
            result = await self.database.fetch_one(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_approvers_by_bill_id(
        self, bill_id: int
    ) -> List[TgBillApproversModel]:
        try:
            query = self.tg_bot_bill_approvers.select().where(
                self.tg_bot_bill_approvers.c.bill_id == bill_id
            )
            result = await self.database.fetch_all(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")

    async def get_approvers_extended_by_bill_id(
        self, bill_id: int
    ) -> List[Union[TgBillApproversExtendedModel, None]]:
        try:
            query = (
                select([self.tg_bot_bill_approvers, self.users.c.username])
                .select_from(self.tg_bot_bill_approvers)
                .join(
                    self.users,
                    self.tg_bot_bill_approvers.c.approver_id == self.users.c.id,
                )
                .where(self.tg_bot_bill_approvers.c.id == bill_id)
            )
            result = await self.database.fetch_all(query)
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error: {e}")
