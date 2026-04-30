from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from database.db import categories, database, pictures
from functions.helpers import datetime_to_timestamp, get_user_by_token
from sqlalchemy import func, select


class GetAllCategoriesView:

    def __init__(self, s3_factory: IS3ServiceFactory):
        self.__s3_factory = s3_factory

    async def __call__(
        self, token: str, limit: int = 100, offset: int = 0, include_photo: bool = False
    ):
        """Получение списка категорий, отсортированных по дате создания"""
        user = await get_user_by_token(token)

        if include_photo:
            # Фото ищутся по entity/category, как для nomenclature
            query = (
                select(categories, pictures.c.url.label("picture"))
                .select_from(categories)
                .outerjoin(
                    pictures,
                    (pictures.c.entity == "categories")
                    & (pictures.c.entity_id == categories.c.id)
                    & (pictures.c.is_deleted.is_not(True)),
                )
                .where(
                    categories.c.cashbox == user.cashbox_id,
                    categories.c.is_deleted.is_not(True),
                )
                .order_by(categories.c.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
        else:
            # Без фото - не делаем JOIN
            query = (
                select(categories)
                .where(
                    categories.c.cashbox == user.cashbox_id,
                    categories.c.is_deleted.is_not(True),
                )
                .order_by(categories.c.created_at.desc())
                .limit(limit)
                .offset(offset)
            )

        categories_db = await database.fetch_all(query)
        categories_db = [*map(datetime_to_timestamp, categories_db)]

        query = select(func.count(categories.c.id)).where(
            categories.c.owner == user.id,
            categories.c.is_deleted.is_not(True),
        )

        categories_db_count = await database.fetch_one(query)

        return {"result": categories_db, "count": categories_db_count.count_1}
