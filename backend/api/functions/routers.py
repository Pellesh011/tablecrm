import api.functions.schemas as schemas
from database.db import database, entity_or_function, status_entity_function
from fastapi import APIRouter, HTTPException
from functions.helpers import (
    check_function_exists,
    datetime_to_timestamp,
    get_user_by_token,
)
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["cashbox_functions"])


@router.get("/cashbox_functions/", response_model=schemas.FunctionListGet)
async def get_user_functions(token: str, limit: int = 100, offset: int = 0):
    """Получение подключённых функций пользователя"""
    user = await get_user_by_token(token)
    query = entity_or_function.select().limit(limit).offset(offset)
    available_functions = await database.fetch_all(query)

    query = (
        status_entity_function.select()
        .where(
            status_entity_function.c.cashbox == user.cashbox_id,
        )
        .limit(limit)
        .offset(offset)
    )
    cashbox_functions = await database.fetch_all(query)
    if cashbox_functions:
        cashbox_functions_names = [
            entity.entity_or_function for entity in cashbox_functions
        ]
        result = [*map(dict, cashbox_functions)]
    else:
        cashbox_functions_names, result = [], []

    for entity in available_functions:
        if entity.name not in cashbox_functions_names:
            result.append({"entity_or_function": entity.name, "status": False})

    query = select(func.count(status_entity_function.c.id)).where(
        status_entity_function.c.cashbox == user.cashbox_id,
    )
    cashbox_functions_c = await database.fetch_one(query)
    return {"result": result, "count": cashbox_functions_c.count_1}


@router.post("/cashbox_functions/", response_model=schemas.Function)
async def activate_function(token: str, function: str):
    """Включение функции"""
    user = await get_user_by_token(token)
    await check_function_exists(function)

    query = status_entity_function.select().where(
        status_entity_function.c.cashbox == user.cashbox_id,
        status_entity_function.c.entity_or_function == function,
    )
    function_db = await database.fetch_one(query)
    if function_db:
        raise HTTPException(status_code=400, detail="Эта функция уже активирована")

    function_values = {
        "entity_or_function": function,
        "status": True,
        "cashbox": user.cashbox_id,
    }
    query = status_entity_function.insert().values(function_values)
    function_id = await database.execute(query)

    query = status_entity_function.select().where(
        status_entity_function.c.cashbox == user.cashbox_id,
        status_entity_function.c.id == function_id,
    )
    function_db = await database.fetch_one(query)
    function_db = datetime_to_timestamp(function_db)

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "cashbox_functions",
            "result": function_db,
        },
    )

    return function_db


@router.delete("/cashbox_functions/", response_model=bool)
async def deactivate_function(token: str, function: str):
    """Отключение функции"""
    user = await get_user_by_token(token)
    await check_function_exists(function)

    query = status_entity_function.select().where(
        status_entity_function.c.cashbox == user.cashbox_id,
        status_entity_function.c.entity_or_function == function,
    )
    function_db = await database.fetch_one(query)

    if not function_db:
        raise HTTPException(status_code=404, detail="Эта функция не была активирована")

    query = status_entity_function.delete().where(
        status_entity_function.c.id == function_db.id
    )
    await database.execute(query)

    return True
