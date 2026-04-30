from api.manufacturers import schemas
from database.db import database, manufacturers
from functions.helpers import datetime_to_timestamp, get_entity_by_id, get_user_by_token
from ws_manager import manager


class PatchManufacturersView:

    async def __call__(
        self, token: str, idx: int, manufacturer: schemas.ManufacturerEdit
    ):
        """Редактирование производителя"""
        user = await get_user_by_token(token)
        manufacturer_db = await get_entity_by_id(manufacturers, idx, user.id)
        manufacturer_values = manufacturer.dict(exclude_unset=True)

        if manufacturer_values:
            query = (
                manufacturers.update()
                .where(manufacturers.c.id == idx, manufacturers.c.owner == user.id)
                .values(manufacturer_values)
            )
            await database.execute(query)
            manufacturer_db = await get_entity_by_id(manufacturers, idx, user.id)

        manufacturer_db = datetime_to_timestamp(manufacturer_db)

        await manager.send_message(
            token,
            {"action": "edit", "target": "manufacturers", "result": manufacturer_db},
        )

        return manufacturer_db
