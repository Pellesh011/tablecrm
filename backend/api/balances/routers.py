from datetime import datetime, timedelta

from api.balances.schemas import AccountInfo, BalanceCreate
from const import BLOCKED, DEMO, PAID
from database.db import (
    accounts_balances,
    database,
    tariffs,
    users,
    users_cboxes_relation,
)
from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select
from texts import url_link_pay

router = APIRouter(tags=["account"])


@router.get("/account/info/", response_model=AccountInfo)
async def get_account_info(token: str):
    """Получение информации об аккаунте и оплате"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            balance = await database.fetch_one(
                accounts_balances.select(accounts_balances.c.cashbox == user.cashbox_id)
            )
            if balance:
                balance_tariff = await database.fetch_one(
                    tariffs.select().where(tariffs.c.id == balance.tariff)
                )
                count_query = select(func.count(users_cboxes_relation.c.id)).where(
                    users_cboxes_relation.c.cashbox_id == balance.cashbox
                )
                users_quantity = await database.execute(count_query)

                if balance.tariff_type == DEMO:
                    demo_expiration = int(
                        (
                            datetime.fromtimestamp(balance.created_at)
                            + timedelta(days=balance_tariff.demo_days)
                        ).timestamp()
                    )
                    demo_left = demo_expiration - datetime.utcnow().timestamp()
                    demo_left = demo_left if demo_left >= 0 else 0
                else:
                    demo_expiration = 0
                    demo_left = 0

                tg_account_info = await database.fetch_one(
                    users.select().where(users.c.id == user.user)
                )

                if not tg_account_info:
                    raise HTTPException(
                        status_code=404,
                        detail="Нет доп. информации о вашем телеграм аккаунте",
                    )

                info = AccountInfo(
                    is_owner=user.get("is_owner"),
                    type=balance.tariff_type,
                    demo_expiration=demo_expiration,
                    demo_left=demo_left,
                    balance=balance.balance,
                    users=users_quantity,
                    price=balance_tariff.price,
                    is_per_user=balance_tariff.per_user,
                    tariff=balance_tariff.name,
                    link_for_pay=url_link_pay.format(
                        user_id=tg_account_info.owner_id, cashbox_id=user.cashbox_id
                    ),
                    demo_period=balance_tariff.demo_days,
                )
                return info
            raise HTTPException(
                status_code=404, detail="Нет доп. информации о вашем аккаунте"
            )
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.post("/account/balance/create", status_code=201)
async def create_balance(token: str, balance_data: BalanceCreate = None):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    existing_balance = await database.fetch_one(
        accounts_balances.select().where(accounts_balances.c.cashbox == user.cashbox_id)
    )

    if existing_balance:
        raise HTTPException(
            status_code=400,
            detail=f"Balance already exists for cashbox {user.cashbox_id}",
        )

    tariff = None
    if balance_data and balance_data.tariff_id:
        tariff = await database.fetch_one(
            tariffs.select().where(tariffs.c.id == balance_data.tariff_id)
        )
        if not tariff:
            raise HTTPException(
                status_code=404,
                detail=f"Tariff with id {balance_data.tariff_id} not found",
            )
    else:
        tariff_query = (
            tariffs.select()
            .where(tariffs.c.actual == True)
            .order_by(tariffs.c.price.asc())
        )
        tariff = await database.fetch_one(tariff_query)

        if not tariff:
            raise HTTPException(status_code=404, detail="No actual tariffs found")

    tariff_type = DEMO
    if balance_data and balance_data.tariff_type:
        if balance_data.tariff_type in [DEMO, PAID, BLOCKED]:
            tariff_type = balance_data.tariff_type
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tariff_type. Must be one of: {DEMO}, {PAID}, {BLOCKED}",
            )

    created = int(datetime.utcnow().timestamp())
    balance_query = accounts_balances.insert().values(
        cashbox=user.cashbox_id,
        balance=0,
        tariff=tariff.id,
        tariff_type=tariff_type,
        created_at=created,
        updated_at=created,
    )
    balance_id = await database.execute(balance_query)

    created_balance = await database.fetch_one(
        accounts_balances.select().where(accounts_balances.c.id == balance_id)
    )

    return {
        "success": True,
        "message": "Balance created successfully",
        "balance": {
            "id": created_balance.id,
            "cashbox": created_balance.cashbox,
            "tariff": created_balance.tariff,
            "balance": created_balance.balance,
            "tariff_type": created_balance.tariff_type,
            "created_at": created_balance.created_at,
        },
    }


@router.get("/tariffs/")
async def get_tariffs(token: str):
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    tariffs_query = (
        tariffs.select()
        .where(
            tariffs.c.actual == True, tariffs.c.archived == False, tariffs.c.price > 0
        )
        .order_by(tariffs.c.price.asc())
    )
    tariffs_db = await database.fetch_all(tariffs_query)

    tariffs_list = [
        {
            "id": t.id,
            "name": t.name,
            "price": t.price,
            "per_user": t.per_user,
            "frequency": t.frequency,
            "demo_days": t.demo_days,
            "offer_hours": t.offer_hours,
            "discount_percent": t.discount_percent,
        }
        for t in tariffs_db
    ]

    return {"result": tariffs_list, "count": len(tariffs_list)}
