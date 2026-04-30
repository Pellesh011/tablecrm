from datetime import datetime, timedelta

from common.decorators import ensure_db_connection
from const import DEMO, PAID
from database.db import accounts_balances, database, tariffs
from functions.account import make_account


@ensure_db_connection
async def check_account():
    balances = await database.fetch_all(accounts_balances.select())
    for balance in balances:
        if balance.tariff_type == DEMO:
            tariff = await database.fetch_one(
                tariffs.select().where(tariffs.c.id == balance.tariff)
            )
            if datetime.utcnow() >= datetime.fromtimestamp(
                balance.created_at
            ) + timedelta(days=tariff.demo_days):
                await make_account(balance)
        elif balance.tariff_type == PAID:
            await make_account(balance)
