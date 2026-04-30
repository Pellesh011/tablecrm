import asyncio

import api.categories.schemas as schemas
from database.db import categories, database, nomenclature, pictures
from fastapi import APIRouter, HTTPException
from functions.helpers import (
    check_entity_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
)
from memoization import CachingAlgorithmFlag, cached
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["categories"])


@router.get("/categories/{idx}/", response_model=schemas.Category)
async def get_category_by_id(token: str, idx: int):
    """Получение категории по ID"""

    user = await get_user_by_token(token)
    category_db = await get_entity_by_id(categories, idx, user.cashbox_id)
    category_db = datetime_to_timestamp(category_db)
    return category_db


@router.delete("/categories/{idx}/photo", response_model=schemas.Category)
async def delete_category_photo(token: str, idx: int):
    """Установка в pictures is_deleted=True и установка category.photo_id = None"""
    user = await get_user_by_token(token)
    # Делаем запрос к categories, чтобы получить photo_id до удаления
    query = categories.select().where(
        categories.c.id == idx,
        categories.c.cashbox == user.cashbox_id,
        categories.c.photo_id.is_not(None),
    )
    record_photo_id = await database.fetch_one(query)
    if not record_photo_id:
        raise HTTPException(
            status_code=404, detail="Категория не найдена или вам не принадлежит"
        )
    photo_id = record_photo_id.get("photo_id")

    query = (
        categories.update()
        .where(
            categories.c.id == idx,
            categories.c.cashbox == user.cashbox_id,
            categories.c.photo_id.is_not(None),
        )
        .values({"photo_id": None})
        .returning(categories)
    )
    category_db = await database.fetch_one(query)

    query = (
        pictures.update()
        .where(
            pictures.c.id == photo_id,
            pictures.c.cashbox == user.cashbox_id,
            pictures.c.is_deleted.is_not(True),
        )
        .values({"is_deleted": True})
        .returning(pictures)
    )

    picture_db = await database.fetch_one(query)

    if not picture_db:
        raise HTTPException(
            status_code=404,
            detail="photo_id удалена, но фотография не найдена или вам не принадлежит",
        )

    category_db = datetime_to_timestamp(category_db)

    return category_db


async def build_hierarchy(data, parent_id=None, name=None):
    @cached(max_size=128, algorithm=CachingAlgorithmFlag.FIFO, thread_safe=False)
    async def build_children(parent_id):

        children = []
        for item in data:
            item = datetime_to_timestamp(item)
            item["children"] = []
            item["key"] = item["id"]

            nomenclature_in_category = (
                select(func.count(nomenclature.c.id).label("nom_count"))
                .where(
                    nomenclature.c.category == item.get("id"),
                    nomenclature.c.name.ilike(f"%{name}%") if name else True,
                )
                .group_by(nomenclature.c.category)
            )
            nomenclature_in_category_result = await database.fetch_one(
                nomenclature_in_category
            )

            item["nom_count"] = (
                0
                if not nomenclature_in_category_result
                else nomenclature_in_category_result.nom_count
            )

            item["expanded_flag"] = False
            if item["parent"] == parent_id:
                grandchildren = await build_children(item["id"])
                if grandchildren:
                    item["children"] = grandchildren

                if (item["nom_count"] == 0) and (name is not None):
                    continue
                children.append(item)
        return children

    tasks = [build_children(parent_id)]
    results = await asyncio.gather(*tasks)
    return results[0]


@router.post("/categories/", response_model=schemas.CategoryList)
async def new_categories(token: str, categories_data: schemas.CategoryCreateMass):
    """Создание категорий"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    parents_cache = set()
    exceptions = []
    for category_values in categories_data.dict()["__root__"]:
        category_values["owner"] = user.id
        category_values["cashbox"] = user.cashbox_id

        if category_values.get("parent") is not None:
            if category_values["parent"] not in parents_cache:
                try:
                    await check_entity_exists(
                        categories, category_values["parent"], user.id
                    )
                    parents_cache.add(category_values["parent"])
                except HTTPException as e:
                    exceptions.append(str(category_values) + " " + e.detail)
                    continue

        query = categories.insert().values(category_values)
        category_id = await database.execute(query)
        inserted_ids.add(category_id)

    query = categories.select().where(
        categories.c.owner == user.id, categories.c.id.in_(inserted_ids)
    )
    categories_db = await database.fetch_all(query)
    categories_db = [*map(datetime_to_timestamp, categories_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "categories",
            "result": categories_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return categories_db


@router.patch("/categories/{idx}/", response_model=schemas.Category)
async def edit_category(
    token: str,
    idx: int,
    category: schemas.CategoryEdit,
):
    """Редактирование категории"""
    user = await get_user_by_token(token)
    category_db = await get_entity_by_id(categories, idx, user.cashbox_id)
    category_values = category.dict(exclude_unset=True)

    if category_values:
        if category_values.get("parent") is not None:
            await check_entity_exists(categories, category_values["parent"], user.id)

        query = (
            categories.update()
            .where(categories.c.id == idx, categories.c.cashbox == user.cashbox_id)
            .values(category_values)
        )
        await database.execute(query)
        category_db = await get_entity_by_id(categories, idx, user.cashbox_id)

    category_db = datetime_to_timestamp(category_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "categories", "result": category_db},
    )

    return category_db


@router.delete("/categories/{idx}/", response_model=schemas.Category)
async def delete_category(token: str, idx: int):
    """Удаление категории"""
    user = await get_user_by_token(token)

    await get_entity_by_id(categories, idx, user.cashbox_id)

    query = (
        categories.update()
        .where(categories.c.id == idx, categories.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = categories.select().where(
        categories.c.id == idx, categories.c.cashbox == user.cashbox_id
    )
    category_db = await database.fetch_one(query)
    category_db = datetime_to_timestamp(category_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "categories",
            "result": category_db,
        },
    )

    return category_db
