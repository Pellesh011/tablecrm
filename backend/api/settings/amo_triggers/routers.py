import uuid

from database.db import amo_bots, amo_install_table_cashboxes, database, table_triggers
from database.enums import TriggerTime
from fastapi import APIRouter, Depends, HTTPException, Request
from functions.helpers import get_user_by_token
from sqlalchemy import func, select, update

from . import schemas

router = APIRouter(tags=["amo_triggers"], prefix="/settings")


async def verify_user(req: Request):
    user = await get_user_by_token(req.query_params["token"])
    if user:
        return user


@router.get("/triggers")
async def get_triggers_list(
    token: str, limit: int = 5, offset: int = 0, user=Depends(verify_user)
):
    try:
        query = select(table_triggers).where(
            table_triggers.c.cashbox_id == user.get("cashbox_id"),
            table_triggers.c.is_deleted.is_not(True),
        )
        triggers = await database.fetch_all(
            query.limit(limit).offset(offset).order_by(table_triggers.c.created_at)
        )
        triggers = [
            schemas.ViewTrigger(
                **{
                    **trigger,
                    "amo_bot": await database.fetch_one(
                        select(amo_bots)
                        .join(
                            amo_install_table_cashboxes,
                            amo_install_table_cashboxes.c.amo_install_group_id
                            == amo_bots.c.install_group_id,
                        )
                        .where(
                            amo_bots.c.id == trigger.get("amo_bots_id"),
                            amo_install_table_cashboxes.c.cashbox_id
                            == user.get("cashbox_id"),
                        )
                    ),
                    "time": (
                        trigger.get("time") / 60
                        if trigger.get("time_variant") == TriggerTime.minute
                        else (
                            trigger.get("time") / 3600
                            if trigger.get("time_variant") == TriggerTime.hour
                            else (
                                trigger.get("time") / 86400
                                if trigger.get("time_variant") == TriggerTime.day
                                else 0
                            )
                        )
                    ),
                }
            )
            for trigger in triggers
        ]
        total = await database.fetch_val(select(func.count()).select_from(query))
        return {"items": triggers, "pageSize": limit, "total": total}
    except Exception as e:

        raise HTTPException(
            status_code=432, detail={"error_code": 500, "error": str(e)}
        )


@router.get("/amo_bots")
async def get_amo_bots(
    token: str, filter: schemas.Filtersamobot = Depends(), user=Depends(verify_user)
):
    filters = []
    try:
        if filter.name:
            filters.append(amo_bots.c.name.ilike(f"%{filter.name}%"))
        query = (
            select(amo_bots)
            .select_from(amo_bots)
            .join(
                amo_install_table_cashboxes,
                amo_install_table_cashboxes.c.amo_install_group_id
                == amo_bots.c.install_group_id,
            )
            .where(amo_install_table_cashboxes.c.cashbox_id == user.get("cashbox_id"))
            .filter(*filters)
            .limit(5)
        )
        bots = await database.fetch_all(query)
        return bots
    except Exception as e:
        raise HTTPException(
            status_code=432, detail={"error_code": 500, "error": str(e)}
        )


@router.post("/triggers", status_code=201)
async def post_trigger(
    token: str, trigger: schemas.CreateTrigger, user=Depends(verify_user)
):
    try:
        trigger_create = {
            "cashbox_id": user.get("cashbox_id"),
            **trigger.dict(),
            "key": str(uuid.uuid4().hex),
            "time": (
                trigger.time * 60
                if trigger.time_variant == TriggerTime.minute
                else (
                    trigger.time * 3600
                    if trigger.time_variant == TriggerTime.hour
                    else (
                        trigger.time * 86400
                        if trigger.time_variant == TriggerTime.day
                        else 0
                    )
                )
            ),
            "is_deleted": False,
        }
        query = table_triggers.insert().values(trigger_create)
        trigger_id = await database.execute(query)
        trigger_created = await database.fetch_one(
            table_triggers.select().where(table_triggers.c.id == trigger_id)
        )
        return trigger_created
    except Exception as e:
        raise HTTPException(
            status_code=432, detail={"error_code": 500, "error": str(e)}
        )


@router.patch("/triggers/{idx}")
async def patch_trigger(
    token: str, idx: int, trigger: schemas.PatchTrigger, user=Depends(verify_user)
):
    try:
        stored_item_data = await database.fetch_one(
            table_triggers.select().where(
                table_triggers.c.id == idx, table_triggers.c.is_deleted.is_not(True)
            )
        )
        if stored_item_data:
            stored_item_model = schemas.PatchTrigger(**stored_item_data)
            update_data = trigger.dict(exclude_unset=True)
            if update_data.get("time"):
                updated_item = {
                    **stored_item_model.copy(update=update_data).dict(),
                    "time": (
                        stored_item_model.copy(update=update_data).dict().get("time")
                        * 60
                        if stored_item_model.copy(update=update_data)
                        .dict()
                        .get("time_variant")
                        == TriggerTime.minute
                        else (
                            stored_item_model.copy(update=update_data)
                            .dict()
                            .get("time")
                            * 3600
                            if stored_item_model.copy(update=update_data)
                            .dict()
                            .get("time_variant")
                            == TriggerTime.hour
                            else (
                                stored_item_model.copy(update=update_data)
                                .dict()
                                .get("time")
                                * 86400
                                if stored_item_model.copy(update=update_data)
                                .dict()
                                .get("time_variant")
                                == TriggerTime.day
                                else 0
                            )
                        )
                    ),
                }
            else:
                updated_item = {**stored_item_model.copy(update=update_data).dict()}

            await database.execute(
                update(table_triggers)
                .values({**updated_item})
                .where(table_triggers.c.id == stored_item_data.get("id"))
            )
            return {
                **updated_item,
                "time": (
                    updated_item.get("time") / 60
                    if updated_item.get("time_variant") == TriggerTime.minute
                    else (
                        updated_item.get("time") / 3600
                        if updated_item.get("time_variant") == TriggerTime.hour
                        else (
                            updated_item.get("time") / 86400
                            if updated_item.get("time_variant") == TriggerTime.day
                            else 0
                        )
                    )
                ),
            }
        else:
            raise Exception("not found trigger")
    except Exception as e:
        raise HTTPException(status_code=432, detail=str(e))
    pass


@router.delete("/triggers/{idx}", status_code=200)
async def delete_trigger(token: str, idx: int, user=Depends(verify_user)):
    try:
        trigger_db = await database.fetch_one(
            table_triggers.select().where(
                table_triggers.c.id == idx, table_triggers.c.is_deleted.is_not(True)
            )
        )
        if trigger_db:
            query = (
                update(table_triggers)
                .values({**trigger_db, "is_deleted": True})
                .where(table_triggers.c.id == trigger_db.get("id"))
            )
            await database.execute(query)
            return {**trigger_db, "is_deleted": True}
        else:
            raise Exception("not found trigger")
    except Exception as e:
        raise HTTPException(status_code=432, detail=str(e))
