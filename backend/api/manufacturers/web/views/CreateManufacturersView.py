from api.manufacturers import schemas
from database.db import database, manufacturers
from functions.helpers import datetime_to_timestamp, get_user_by_token
from ws_manager import manager


class CreateManufacturersView:

    async def __call__(
        self, token: str, manufacturers_data: schemas.ManufacturerCreateMass
    ):
        """Создание производителя"""
        user = await get_user_by_token(token)

        inserted_ids = set()
        for manufacturer_values in manufacturers_data.dict()["__root__"]:
            manufacturer_values["owner"] = user.id
            manufacturer_values["cashbox"] = user.cashbox_id

            query = manufacturers.insert().values(manufacturer_values)
            manufacturer_id = await database.execute(query)
            inserted_ids.add(manufacturer_id)

        query = manufacturers.select().where(
            manufacturers.c.owner == user.id, manufacturers.c.id.in_(inserted_ids)
        )
        manufacturers_db = await database.fetch_all(query)
        manufacturers_db = [*map(datetime_to_timestamp, manufacturers_db)]

        await manager.send_message(
            token,
            {
                "action": "create",
                "target": "manufacturers",
                "result": manufacturers_db,
            },
        )

        return manufacturers_db
