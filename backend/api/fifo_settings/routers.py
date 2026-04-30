from asyncpg import UniqueViolationError
from database.db import database, fifo_settings
from fastapi import APIRouter, HTTPException
from functions.helpers import datetime_to_timestamp, get_user_by_token
from ws_manager import manager

from . import schemas

router = APIRouter(tags=["fifo_settings"])


@router.get("/fifo_settings/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, organization_id: int):
    """Получение настроек по ID организации"""
    await get_user_by_token(token)
    query = fifo_settings.select().where(
        fifo_settings.c.organization_id == organization_id
    )
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail="Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)

    return instance_db


@router.post("/fifo_settings/", response_model=schemas.View)
async def create(token: str, fifo_settings_data: schemas.Create):
    """Создание настроек организации"""
    await get_user_by_token(token)

    query = fifo_settings.insert().values(fifo_settings_data.dict())
    try:
        instance_id = await database.execute(query)
    except UniqueViolationError:
        raise HTTPException(
            400, "Настройки организации уже существуют, вы можете отредактировать их"
        )

    query = fifo_settings.select().where(fifo_settings.c.id == instance_id)
    instance = await database.fetch_one(query)
    instance = datetime_to_timestamp(instance)

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "fifo_settings",
            "result": instance,
        },
    )

    return instance


@router.patch("/fifo_settings/{organization_id}/", response_model=schemas.View)
async def update(token: str, organization_id: int, fifo_settings_data: schemas.Edit):
    """Редактирование настроек организации"""
    await get_user_by_token(token)

    query = fifo_settings.select().where(
        fifo_settings.c.organization_id == organization_id
    )
    if not await database.fetch_one(query):
        raise HTTPException(404, "Нет настроек организации")

    query = (
        fifo_settings.update()
        .where(fifo_settings.c.organization_id == organization_id)
        .values(fifo_settings_data.dict(exclude_unset=True))
    )
    await database.execute(query)

    query = fifo_settings.select().where(
        fifo_settings.c.organization_id == organization_id
    )
    fifo_settings_db = await database.fetch_one(query)
    fifo_settings_db = datetime_to_timestamp(fifo_settings_db)

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "fifo_settings",
            "result": fifo_settings_db,
        },
    )

    return fifo_settings_db
