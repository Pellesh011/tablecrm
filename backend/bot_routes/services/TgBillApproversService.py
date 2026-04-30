from aiogram import types
from bot_routes.functions.TgBillsFuncions import (
    get_user_from_db,
    get_user_from_db_by_username,
)
from bot_routes.models.TgBillApproversModels import (
    TgBillApproversCreateModel,
    TgBillApproversUpdateModel,
)
from bot_routes.repositories.impl.TgBillApproversRepository import (
    TgBillApproversRepository,
)
from database.db import TgBillApproveStatus


class TgBillApproversService:
    def __init__(
        self,
        bill_approvers_repository: TgBillApproversRepository,
    ):
        self.bill_approvers_repository = bill_approvers_repository

    async def get_approve_by_bill_id_and_approver_id(
        self, bill_id: int, approver_id: str
    ):
        approve = (
            await self.bill_approvers_repository.get_approve_by_bill_id_and_approver_id(
                bill_id, approver_id
            )
        )
        return approve

    async def get_bill_approvers(self, bill_id: int):
        approvers = await self.bill_approvers_repository.get_approvers_by_bill_id(
            bill_id
        )
        if not approvers:
            return []
        return approvers

    async def approve(self, bill_id: int, tg_id_updated_by: str):

        user = await get_user_from_db(tg_id_updated_by)
        approve = (
            await self.bill_approvers_repository.get_approve_by_bill_id_and_approver_id(
                bill_id, user.id
            )
        )
        if not approve:
            return False, "Вы не можете подтвердить этот счет"

        if approve.status != TgBillApproveStatus.APPROVED:
            await self.bill_approvers_repository.update(
                approve.id,
                TgBillApproversUpdateModel(status=TgBillApproveStatus.APPROVED),
            )
            return True, "Вы подтвердили счет"
        return False, "Вы уже подтвердили этот счет"

    async def rejet(self, bill_id: int, tg_id_updated_by: str):

        user = await get_user_from_db(tg_id_updated_by)
        approve = (
            await self.bill_approvers_repository.get_approve_by_bill_id_and_approver_id(
                bill_id, user.id
            )
        )
        if not approve:
            return False, "Вы не можете отклонить этот счет"

        await self.bill_approvers_repository.update(
            approve.id, TgBillApproversUpdateModel(status=TgBillApproveStatus.CANCELED)
        )
        return True, "Вы отклонили счет"

    async def create_bill_approvers(self, message: types.Message, bill_id: int):
        bill_approvers = []
        if message.caption_entities:
            for entity in message.caption_entities:
                if entity.type == "mention":
                    username = message.caption[
                        entity.offset + 1 : entity.offset + entity.length
                    ]
                    user = await get_user_from_db_by_username(username)
                    if user:
                        bill_approver = TgBillApproversCreateModel(
                            bill_id=bill_id,
                            approver_id=user.id,
                            status=TgBillApproveStatus.NEW,
                        )
                        approve_id = await self.bill_approvers_repository.insert(
                            bill_approver
                        )
                        bill_approvers.append(
                            {
                                "approver_id": user.id,
                                "username": user.username,
                                "id": approve_id,
                                "status": "new",
                            }
                        )
                    else:
                        return False, "Пользователь не найден"
            return True, "Успешно"
        else:
            return False, "Не указаны пользователи"
