from datetime import datetime

from database.db import cashbox_settings, database
from fastapi import APIRouter, Depends, HTTPException, Request
from functions.helpers import get_user_by_token
from sqlalchemy import select, update

from . import schemas

router = APIRouter(tags=["cashbox_settings"], prefix="/cashbox")


async def verify_user(req: Request):
    user = await get_user_by_token(req.query_params["token"])
    if user:
        return user


@router.get("/settings")
async def get_cashbox_settings(token: str, user=Depends(verify_user)):
    try:
        query = select(cashbox_settings).where(
            cashbox_settings.c.cashbox_id == user.get("cashbox_id"),
            cashbox_settings.c.is_deleted.is_not(True),
        )
        settings = await database.fetch_one(query)
        if not settings:
            raise HTTPException(status_code=404, detail="Settings not found")
        return schemas.CashboxSettingsView(**settings)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/settings", status_code=201)
async def create_cashbox_settings(
    token: str, settings: schemas.CreateCashboxSettings, user=Depends(verify_user)
):
    try:
        now = datetime.utcnow()
        settings_create = {
            "cashbox_id": user.get("cashbox_id"),
            **settings.dict(),
            "created_at": now,
            "updated_at": now,
            "is_deleted": False,
        }
        query = (
            cashbox_settings.insert()
            .values(settings_create)
            .returning(cashbox_settings.c.cashbox_id)
        )
        settings_id = await database.execute(query)
        created = await database.fetch_one(
            select(cashbox_settings).where(cashbox_settings.c.cashbox_id == settings_id)
        )
        return schemas.CashboxSettingsView(**created)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/settings/{idx}")
async def patch_cashbox_settings(
    token: str,
    idx: int,
    settings: schemas.PatchCashboxSettings,
    user=Depends(verify_user),
):
    try:
        stored_item_data = await database.fetch_one(
            select(cashbox_settings).where(
                cashbox_settings.c.cashbox_id == idx,
                cashbox_settings.c.is_deleted.is_not(True),
            )
        )
        if not stored_item_data:
            raise HTTPException(status_code=404, detail="Settings not found")
        update_data = settings.dict(exclude_unset=True)
        update_data["updated_at"] = datetime.utcnow()
        await database.execute(
            update(cashbox_settings)
            .values(update_data)
            .where(cashbox_settings.c.cashbox_id == idx)
        )
        updated_item = {**stored_item_data, **update_data}
        return updated_item
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/settings/{idx}")
async def delete_cashbox_settings(token: str, idx: int, user=Depends(verify_user)):
    try:
        stored_item_data = await database.fetch_one(
            select(cashbox_settings).where(
                cashbox_settings.c.id == idx, cashbox_settings.c.is_deleted.is_not(True)
            )
        )
        if not stored_item_data:
            raise HTTPException(status_code=404, detail="Settings not found")
        await database.execute(
            update(cashbox_settings)
            .values({"is_deleted": True})
            .where(cashbox_settings.c.id == idx)
        )
        return {**stored_item_data, "is_deleted": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
