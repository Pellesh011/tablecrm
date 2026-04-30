from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from database.db import database, manufacturers, pictures
from functions.helpers import datetime_to_timestamp, get_user_by_token
from sqlalchemy import func, select


class GetManufacturersView:

    def __init__(self, s3_factory: IS3ServiceFactory):
        self.__s3_factory = s3_factory

    async def __call__(self, token: str, limit: int = 100, offset: int = 0):
        """Получение списка производителей"""
        s3_client = self.__s3_factory()

        user = await get_user_by_token(token)
        query = (
            select(manufacturers, pictures.c.url.label("picture"))
            .outerjoin(pictures, manufacturers.c.photo_id == pictures.c.id)
            .where(
                manufacturers.c.owner == user.id,
                manufacturers.c.cashbox == user.cashbox_id,
                manufacturers.c.is_deleted.is_not(True),
            )
            .limit(limit)
            .offset(offset)
        )

        manufacturers_db = await database.fetch_all(query)
        manufacturers_db = [*map(datetime_to_timestamp, manufacturers_db)]
        for manufacturer in manufacturers_db:
            if manufacturer.get("picture"):
                try:
                    url = await s3_client.get_link_object(
                        bucket_name="5075293c-docs_generated",
                        file_key=manufacturer.get("picture"),
                    )
                    manufacturer["picture"] = url
                except Exception as e:
                    print(e)

        query = select(func.count(manufacturers.c.id)).where(
            manufacturers.c.owner == user.id,
            manufacturers.c.cashbox == user.cashbox_id,
            manufacturers.c.is_deleted.is_not(True),
        )

        manufacturers_db_count = await database.fetch_one(query)

        return {"result": manufacturers_db, "count": manufacturers_db_count.count_1}
