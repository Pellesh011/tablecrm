from database.db import database, distribution_docs, distribution_docs_operations
from fastapi import APIRouter, HTTPException
from functions.helpers import datetime_to_timestamp, get_user_by_token
from sqlalchemy import func, select
from ws_manager import manager

from . import schemas

router = APIRouter(tags=["distribution_docs"])


@router.get("/distribution_docs/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)
    query = distribution_docs.select().where(
        distribution_docs.c.id == idx, distribution_docs.c.is_deleted.is_not(True)
    )
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail="Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)

    query = distribution_docs_operations.select().where(
        distribution_docs_operations.c.distribution_fifo == idx
    )
    table_db = await database.fetch_all(query)
    instance_db["table"] = table_db

    return instance_db


@router.get("/distribution_docs/", response_model=schemas.ListViewGet)
async def get_list(token: str, limit: int = 100, offset: int = 0):
    """Получение списка документов"""
    await get_user_by_token(token)
    query = (
        distribution_docs.select()
        .where(distribution_docs.c.is_deleted.is_not(True))
        .limit(limit)
        .offset(offset)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    query = select(func.count(distribution_docs.c.id)).where(
        distribution_docs.c.is_deleted.is_not(True)
    )
    items_db_c = await database.fetch_one(query)

    return {"result": items_db, "count": items_db_c.count_1}


@router.delete("/distribution_docs/{idx}", response_model=schemas.ListView)
async def delete(token: str, ids: list[int]):
    """Удаление документов"""
    await get_user_by_token(token)

    query = distribution_docs.select().where(
        distribution_docs.c.id.in_(ids), distribution_docs.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            distribution_docs.update()
            .where(
                distribution_docs.c.id.in_(ids),
                distribution_docs.c.is_deleted.is_not(True),
            )
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "distribution_docs",
                "result": items_db,
            },
        )

    return items_db
