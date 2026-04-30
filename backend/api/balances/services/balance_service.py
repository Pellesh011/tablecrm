from datetime import datetime

from const import PAID, SUCCESS, PaymentType
from database.db import accounts_balances, cboxes, database, pboxes, transactions
from sqlalchemy import func, select


async def make_deposit(
    cashbox_id: int,
    amount: float,
    tariff_id: int,
    users_quantity: int,
    is_manual: bool = False,
    status: str = SUCCESS,
) -> int:
    now = datetime.utcnow()

    balance_query = accounts_balances.select().where(
        accounts_balances.c.cashbox == cashbox_id
    )
    balance = await database.fetch_one(balance_query)

    if not balance:
        balance_query = accounts_balances.insert().values(
            cashbox=cashbox_id,
            tariff=tariff_id,
            balance=0,
            tariff_type=PAID,
            created_at=int(now.timestamp()),
            updated_at=int(now.timestamp()),
        )
        balance_id = await database.execute(balance_query)
        balance = await database.fetch_one(
            accounts_balances.select().where(accounts_balances.c.id == balance_id)
        )

    old_balance_value = balance.balance
    new_balance = old_balance_value + amount
    update_query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values(
            {
                "balance": new_balance,
                "tariff_type": PAID,
                "updated_at": int(now.timestamp()),
            }
        )
    )
    await database.execute(update_query)

    transaction_query = transactions.insert().values(
        {
            "cashbox": cashbox_id,
            "tariff": tariff_id,
            "users": users_quantity,
            "amount": amount,
            "status": status,
            "type": PaymentType.incoming,
            "is_manual_deposit": is_manual,
            "created_at": int(now.timestamp()),
            "updated_at": int(now.timestamp()),
        }
    )
    transaction_id = await database.execute(transaction_query)

    update_query = (
        accounts_balances.update()
        .where(accounts_balances.c.id == balance.id)
        .values({"last_transaction": transaction_id})
    )
    await database.execute(update_query)

    await database.execute(
        cboxes.update()
        .where(cboxes.c.id == cashbox_id)
        .values({"balance": new_balance, "updated_at": int(now.timestamp())})
    )

    return transaction_id


async def update_cashbox_balance(cashbox_id: int) -> None:
    try:
        sum_query = select(func.sum(pboxes.c.balance)).where(
            pboxes.c.cashbox == cashbox_id
        )
        total_balance = await database.execute(sum_query)

        if total_balance is None:
            total_balance = 0.0
        else:
            total_balance = round(float(total_balance), 2)

        current_cbox = await database.fetch_one(
            cboxes.select().where(cboxes.c.id == cashbox_id)
        )
        old_balance = current_cbox.balance if current_cbox else 0.0

        if old_balance != total_balance:
            update_query = (
                cboxes.update()
                .where(cboxes.c.id == cashbox_id)
                .values(
                    {
                        "balance": total_balance,
                        "updated_at": int(datetime.utcnow().timestamp()),
                    }
                )
            )
            await database.execute(update_query)
    except Exception as e:
        import traceback

        print(f"Error updating cashbox balance for cashbox {cashbox_id}: {e}")
        print(f"Traceback: {traceback.format_exc()}")
