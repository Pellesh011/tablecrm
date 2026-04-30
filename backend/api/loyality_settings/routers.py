from datetime import datetime
from typing import Optional

import api.loyality_settings.schemas as schemas
from database.db import database, loyality_settings
from fastapi import APIRouter, HTTPException
from functions.helpers import get_user_by_token
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["loyality_settings"])


@router.get("/loyality_settings/")
async def get_loyality_settings(token: str, organization_id: Optional[int] = None):
    user = await get_user_by_token(token)

    settings = None
    res = []
    filters = [loyality_settings.c.cashbox == user.cashbox_id]

    if organization_id:
        filters.append(loyality_settings.c.organization == organization_id)

    q = loyality_settings.select().where(*filters)
    settings = await database.fetch_all(q)

    q = select(func.count(loyality_settings.c.id)).where(*filters)
    count = await database.fetch_one(q)

    for setting in settings:
        setting_dict = dict(setting)
        setting_dict["created_at"] = int(setting.created_at.timestamp())
        setting_dict["updated_at"] = int(setting.updated_at.timestamp())
        setting_dict["start_period"] = int(setting.start_period.timestamp())
        setting_dict["end_period"] = int(setting.end_period.timestamp())

        res.append(setting_dict)

    return {"result": res, "count": count.count_1}


@router.post("/loyality_settings/")
async def create_loyality_setting(token: str, settings_body: schemas.CreateSetting):
    user = await get_user_by_token(token)

    inserted_id = None

    filters = [loyality_settings.c.cashbox == user.cashbox_id]

    body = {
        "cashbox": user.cashbox_id,
        "tags": settings_body.tags,
        "cashback_percent": settings_body.cashback_percent,
        "minimal_checque_amount": settings_body.minimal_checque_amount,
        "start_period": datetime.fromtimestamp(settings_body.start_period),
        "end_period": datetime.fromtimestamp(settings_body.end_period),
        "max_withdraw_percentage": settings_body.max_withdraw_percentage,
        "max_percentage": settings_body.max_percentage,
        "lifetime": settings_body.lifetime,
    }

    if settings_body.organization is not None:
        filters.append(loyality_settings.c.organization == settings_body.organization)
        body["organization"] = settings_body.organization

    q = loyality_settings.select().where(*filters)
    existing = await database.fetch_one(q)

    if existing:
        raise HTTPException(403, "Такая настройка уже существует")

    q = loyality_settings.insert().values(body)
    inserted_id = await database.execute(q)

    q = loyality_settings.select().where(loyality_settings.c.id == inserted_id)
    inserted = await database.fetch_one(q)

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "loyality_settings",
            "result": inserted,
        },
    )

    return inserted


@router.patch("/loyality_settings/")
async def edit_loyality_setting(token: str, settings_body: schemas.EditSetting):
    user = await get_user_by_token(token)

    filters = [loyality_settings.c.cashbox == user.cashbox_id]

    if settings_body.organization is not None:
        filters.append(loyality_settings.c.organization == settings_body.organization)

    body = settings_body.dict()
    del body["organization"]
    body["start_period"] = datetime.fromtimestamp(settings_body.start_period)
    body["end_period"] = datetime.fromtimestamp(settings_body.end_period)

    q = loyality_settings.select().where(*filters)
    finded = await database.fetch_one(q)

    q = (
        loyality_settings.update()
        .where(loyality_settings.c.id == finded.id)
        .values(body)
    )
    await database.execute(q)

    q = loyality_settings.select().where(loyality_settings.c.id == finded.id)
    inserted = await database.fetch_one(q)

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "loyality_settings",
            "result": inserted,
        },
    )

    return inserted


@router.delete("/loyality_settings/")
async def edit_loyality_setting(token: str, organization_id: Optional[int] = None):
    user = await get_user_by_token(token)

    filters = [loyality_settings.c.cashbox == user.cashbox_id]

    if organization_id is not None:
        filters.append(loyality_settings.c.organization == organization_id)

    q = loyality_settings.select().where(*filters)
    finded = await database.fetch_one(q)

    q = loyality_settings.delete().where(loyality_settings.c.id == finded.id)
    await database.execute(q)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "loyality_settings",
            "result": finded,
        },
    )

    return finded
