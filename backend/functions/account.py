from datetime import datetime, timedelta

import texts
from api.payments.schemas import PaymentType
from bot import bot
from const import BLOCKED, PAID, SUCCESS
from database.db import (
    accounts_balances,
    database,
    tariffs,
    transactions,
    users,
    users_cboxes_relation,
)
from sqlalchemy import func, select


async def make_transaction(
    balance: accounts_balances, price: int, users_quantity: int
) -> None:
    """Takes money from balance, sets its status to success and creates a new transaction"""
    now = datetime.utcnow()
    query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values(
            {
                "balance": balance.balance - price,
                "tariff_type": PAID,
                "updated_at": int(now.timestamp()),
            }
        )
    )
    await database.execute(query)
    query = transactions.insert(
        {
            "cashbox": balance.cashbox,
            "tariff": balance.tariff,
            "users": users_quantity,
            "amount": price,
            "status": SUCCESS,
            "created_at": int(datetime.utcnow().timestamp()),
            "updated_at": int(datetime.utcnow().timestamp()),
            "type": PaymentType.incoming,
        }
    )
    result = await database.execute(query)
    query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values({"last_transaction": result})
    )
    await database.execute(query)


async def make_account(balance: accounts_balances) -> None:
    """Checks if paid period finished"""
    now = datetime.utcnow()
    balance_tariff = await database.fetch_one(
        tariffs.select().where(tariffs.c.id == balance.tariff)
    )

    # checking if paid period finished or not
    if balance.last_transaction:
        transaction = await database.fetch_one(
            transactions.select().where(transactions.c.id == balance.last_transaction)
        )
        if (
            datetime.fromtimestamp(transaction.updated_at)
            + timedelta(days=balance_tariff.frequency)
            > now
        ):
            return

    count_query = select(func.count(users_cboxes_relation.c.id)).where(
        users_cboxes_relation.c.cashbox_id == balance.cashbox
    )
    users_quantity = await database.execute(count_query)
    price = balance_tariff.price / balance_tariff.frequency
    if balance_tariff.per_user:
        price *= users_quantity

    # making new transaction
    if price and balance.balance >= price:
        await make_transaction(balance, price, users_quantity)
        return

    # if user has insufficient funds blocking account and sending a message
    query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values({"tariff_type": BLOCKED, "updated_at": int(now.timestamp())})
    )
    await database.execute(query)

    get_users_id = select(users_cboxes_relation.c.user).where(
        users_cboxes_relation.c.cashbox_id == balance.cashbox
    )
    query = users.select().where(users.c.id == get_users_id.scalar_subquery())
    user = await database.fetch_one(query)
    chat_id = user.chat_id
    await bot.send_message(
        chat_id,
        texts.balance_blocked.format(
            tariff=balance_tariff.name,
            users=users_quantity,
            per_user=balance_tariff.price if balance_tariff.per_user else 0,
            total=price,
            link=texts.url_link_pay.format(
                user_id=user.owner_id, cashbox_id=balance.cashbox
            ),
        ),
    )
