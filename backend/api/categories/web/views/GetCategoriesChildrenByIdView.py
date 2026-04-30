from api.categories.routers import build_hierarchy
from database.db import categories, database, nomenclature
from fastapi import HTTPException
from functions.helpers import datetime_to_timestamp, get_user_by_token
from sqlalchemy import func, select


class GetCategoriesChildrenByIdView:

    async def __call__(self, token: str, idx: int):
        user = await get_user_by_token(token)
        query = categories.select().where(
            categories.c.id == idx,
            categories.c.cashbox == user.cashbox_id,
            categories.c.is_deleted.is_not(True),
        )
        categories_db = await database.fetch_one(query)
        if not categories_db:
            raise HTTPException(
                status_code=404, detail=f"Категория с id {idx} не найдена"
            )

        category_dict = dict(categories_db)
        category_dict["key"] = category_dict["id"]
        category_dict["expanded_flag"] = False

        nomenclature_in_category = (
            select(func.count(nomenclature.c.id).label("nom_count"))
            .where(nomenclature.c.category == category_dict["id"])
            .group_by(nomenclature.c.category)
        )
        nomenclature_in_category_result = await database.fetch_one(
            nomenclature_in_category
        )

        category_dict["nom_count"] = (
            0
            if not nomenclature_in_category_result
            else nomenclature_in_category_result.nom_count
        )

        query = f"""
                    with recursive categories_hierarchy as (
                    select id, name, parent, description, code, status, updated_at, created_at, 1 as lvl
                    from categories where parent = {category_dict['id']}

                    union
                    select F.id, F.name, F.parent, F.description, F.code, F.status, F.updated_at, F.created_at, H.lvl+1
                    from categories_hierarchy as H
                    join categories as F on F.parent = H.id
                    )
                    select * from categories_hierarchy
                """
        childrens = await database.fetch_all(query)
        if childrens:
            category_dict["children"] = await build_hierarchy(
                [dict(child) for child in childrens], category_dict["id"]
            )
        else:
            category_dict["children"] = []

        categories_db = datetime_to_timestamp(category_dict)
        return {"result": [categories_db], "count": 1}
