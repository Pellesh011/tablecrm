import api.units.schemas as schemas
from database.db import database, units
from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select

router = APIRouter(tags=["units"])


@router.get("/units/", response_model=schemas.UnitListGet)
async def get_available_units(limit: int = 100, offset: int = 0):
    """Получение списка доступных единиц измерения"""
    query = units.select().limit(limit).offset(offset)
    res = await database.fetch_all(query)

    query = select(func.count(units.c.id))
    count = await database.fetch_one(query)

    return {"result": res, "count": count.count_1}


@router.get("/units/{idx}/", response_model=schemas.Unit)
async def get_unit(idx: int):
    """Получение единицы измерения"""
    query = units.select().where(units.c.id == idx)
    entity_db = await database.fetch_one(query)

    if not entity_db:
        raise HTTPException(status_code=404, detail="У вас нет ед. изм. с таким id")

    return entity_db
