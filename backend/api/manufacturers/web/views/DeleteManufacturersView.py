from database.db import database, manufacturers
from functions.helpers import datetime_to_timestamp, get_entity_by_id, get_user_by_token
from ws_manager import manager


class DeleteManufacturersView:

    async def __call__(self, token: str, idx: int):
        """Удаление производителя"""
        user = await get_user_by_token(token)

        await get_entity_by_id(manufacturers, idx, user.cashbox_id)

        query = (
            manufacturers.update()
            .where(
                manufacturers.c.id == idx,
                manufacturers.c.owner == user.id,
                manufacturers.c.cashbox == user.cashbox_id,
            )
            .values({"is_deleted": True})
        )
        await database.execute(query)

        query = manufacturers.select().where(
            manufacturers.c.id == idx,
            manufacturers.c.owner == user.id,
            manufacturers.c.cashbox == user.cashbox_id,
        )
        manufacturer_db = await database.fetch_one(query)
        manufacturer_db = datetime_to_timestamp(manufacturer_db)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "manufacturers",
                "result": manufacturer_db,
            },
        )

        return manufacturer_db
