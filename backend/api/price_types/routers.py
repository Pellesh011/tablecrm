from typing import Optional

import api.price_types.schemas as schemas
from database.db import database, price_types
from fastapi import APIRouter, HTTPException, Query
from functions.helpers import (
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    raise_bad_request,
)
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["price_types"])


@router.get("/price_types/{idx}/", response_model=schemas.PriceType)
async def get_price_type_by_id(token: str, idx: int):
    """Получение типа цен по ID"""
    user = await get_user_by_token(token)
    price_type_db = await get_entity_by_id(price_types, idx, user.cashbox_id)
    price_type_db = datetime_to_timestamp(price_type_db)
    return price_type_db


@router.get("/price_types/", response_model=schemas.PriceTypeListGet)
async def get_price_types(
    token: str,
    limit: int = 100,
    offset: int = 0,
    tags: Optional[str] = Query(None),
    mode: Optional[str] = Query("or"),
):
    """Получение списка типов цен"""
    user = await get_user_by_token(token)

    tag_list = [tag.strip() for tag in tags.split(",")] if tags else None

    if mode not in ("and", "or"):
        raise HTTPException(400, "mode must be 'and' or 'or'")

    query = price_types.select().where(
        price_types.c.cashbox == user.cashbox_id,
        price_types.c.is_deleted.is_not(True),
    )

    if tag_list:
        if mode == "and":
            query = query.where(price_types.c.tags.contains(tag_list))
        else:
            query = query.where(price_types.c.tags.overlap(tag_list))

    query = query.limit(limit).offset(offset)

    price_types_db = await database.fetch_all(query)
    price_types_db = [*map(datetime_to_timestamp, price_types_db)]

    count_query = select(func.count(price_types.c.id)).where(
        price_types.c.cashbox == user.cashbox_id,
        price_types.c.is_deleted.is_not(True),
    )

    if tag_list:
        if mode == "and":
            count_query = count_query.where(price_types.c.tags.contains(tag_list))
        else:
            count_query = count_query.where(price_types.c.tags.overlap(tag_list))

    price_types_db_count = await database.fetch_one(count_query)

    return {"result": price_types_db, "count": price_types_db_count.count_1}


@router.post("/price_types/", response_model=schemas.PriceType)
async def new_price_type(token: str, price_type: schemas.PriceTypeCreate):
    """Создание типа цен"""
    user = await get_user_by_token(token)

    price_type_values = price_type.dict()
    price_type_values["owner"] = user.id
    price_type_values["cashbox"] = user.cashbox_id

    query = price_types.insert().values(price_type_values)
    price_type_id = await database.execute(query)

    price_type_db = await get_entity_by_id(price_types, price_type_id, user.cashbox_id)
    price_type_db = datetime_to_timestamp(price_type_db)

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "price_types",
            "result": price_type_db,
        },
    )

    return price_type_db


@router.patch("/price_types/{idx}/", response_model=schemas.PriceType)
async def edit_price_type(
    token: str,
    idx: int,
    price_type: schemas.PriceTypeEdit,
):
    """Редактирование типа цен"""
    user = await get_user_by_token(token)
    price_type_db = await get_entity_by_id(price_types, idx, user.cashbox_id)

    if price_type_db.is_system:
        raise_bad_request("Невозможно редактировать системный вид цены")

    price_type_values = price_type.dict(exclude_unset=True)

    if price_type_values:
        query = (
            price_types.update()
            .where(price_types.c.id == idx, price_types.c.cashbox == user.cashbox_id)
            .values(price_type_values)
        )
        await database.execute(query)
        price_type_db = await get_entity_by_id(price_types, idx, user.id)

    price_type_db = datetime_to_timestamp(price_type_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "price_types", "result": price_type_db},
    )

    return price_type_db


@router.delete("/price_types/{idx}/", response_model=schemas.PriceType)
async def delete_price_type(token: str, idx: int):
    """Удаление типа цен"""
    user = await get_user_by_token(token)

    price_type_db = await get_entity_by_id(price_types, idx, user.cashbox_id)

    if price_type_db.is_system:
        raise_bad_request("Невозможно удалить системный вид цены")

    query = (
        price_types.update()
        .where(price_types.c.id == idx, price_types.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = price_types.select().where(
        price_types.c.id == idx, price_types.c.cashbox == user.cashbox_id
    )
    price_type_db = await database.fetch_one(query)
    price_type_db = datetime_to_timestamp(price_type_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "price_types",
            "result": price_type_db,
        },
    )

    return price_type_db
