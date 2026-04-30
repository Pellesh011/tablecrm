from datetime import datetime

from api.payments.routers import create_payment
from api.payments.schemas import PaymentCreate
from database.db import database, payments, users_cboxes_relation


async def clear_repeats(payment_id):
    query = (
        payments.update()
        .where(payments.c.id == payment_id)
        .values(
            {
                "repeat_period": None,
            }
        )
    )
    await database.execute(query)


async def repeat_payment(payment, parent_id):
    payment_dict = dict(payment)
    payment_dict["repeat"] = {
        "repeat_period": None,
        "repeat_first": None,
        "repeat_last": None,
        "repeat_number": None,
        "repeat_seconds": None,
        "repeat_day": None,
        "repeat_month": None,
        "repeat_weekday": None,
        "repeat_parent_id": parent_id,
    }
    try:
        del payment_dict["cheque"]
    except KeyError:
        pass
    del payment_dict["id"]
    payment_dict["status"] = False
    payment_dict["date"] = datetime.utcnow().timestamp()

    query = users_cboxes_relation.select().filter(
        users_cboxes_relation.c.cashbox_id == payment.cashbox
    )
    user_info = await database.fetch_one(query)
    token = user_info.token

    await create_payment(token, PaymentCreate(**payment_dict))
