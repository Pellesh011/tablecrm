from typing import Union

from bot_routes.models.TgBillsModels import (
    TgBillsCreateModel,
    TgBillsExtendedModel,
    TgBillsUpdateModel,
)


class ITgBillsRepository:
    async def update(self, id: int, bill: TgBillsUpdateModel) -> None:
        raise NotImplementedError

    async def insert(self, bill: TgBillsCreateModel) -> int:  # Return the inserted ID
        raise NotImplementedError

    async def delete(self, id: int) -> None:
        raise NotImplementedError

    async def get_by_id(self, id: int) -> Union[TgBillsExtendedModel, None]:
        raise NotImplementedError
