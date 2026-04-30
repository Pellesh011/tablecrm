import json
from datetime import datetime

import websockets
from database.db import (
    cashbox_settings,
    cboxes,
    database,
    pboxes,
    price_types,
    users,
    users_cboxes_relation,
)
from functions.helpers import gen_token


async def create_cbox(user):
    created = int(datetime.utcnow().timestamp())
    updated = int(datetime.utcnow().timestamp())

    created_date = datetime.utcnow().date()
    created_date_ts = int(
        datetime.timestamp(datetime.combine(created_date, datetime.min.time()))
    )

    invite_token = gen_token()
    cashbox_query = cboxes.insert().values(
        balance=0,
        admin=user.id,
        invite_token=invite_token,
        created_at=created,
        updated_at=updated,
    )

    cashbox_id = await database.execute(cashbox_query)

    settings_query = cashbox_settings.insert().values(
        cashbox_id=cashbox_id,
        require_photo_for_writeoff=False,
        check_nomenclature_duplicates_by_name=False,
        check_nomenclature_duplicates_by_code=False,
        created_at=datetime.now(),
        updated_at=datetime.now(),
        is_deleted=False,
    )
    await database.execute(settings_query)

    cashbox_query = cboxes.select().where(cboxes.c.id == cashbox_id)
    cashbox = await database.fetch_one(cashbox_query)

    paybox = pboxes.insert().values(
        start_balance=0,
        balance=0,
        name="default",
        balance_date=created_date_ts,
        update_start_balance=created_date_ts,
        update_start_balance_date=created_date_ts,
        cashbox=cashbox.id,
        created_at=created,
        updated_at=created,
    )

    cbox_update_name = (
        cboxes.update()
        .where(cboxes.c.id == cashbox.id)
        .values(name=f"{user.first_name}_{cashbox.id}")
    )
    await database.execute(cbox_update_name)
    await database.execute(paybox)

    rel_token = gen_token()

    relship = users_cboxes_relation.insert().values(
        user=user.id,
        cashbox_id=cashbox.id,
        token=rel_token,
        status=True,
        is_owner=True,
        created_at=created,
        updated_at=updated,
    )

    rl_id = await database.execute(relship)
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.id == rl_id)
    rl = await database.fetch_one(query)

    query = price_types.insert().values(
        name="chatting", owner=rl.id, cashbox=cashbox.id, is_system=True
    )
    await database.execute(query)

    return rl


async def join_cbox(user, cbox):
    rel_token = gen_token()

    created = int(datetime.utcnow().timestamp())
    updated = int(datetime.utcnow().timestamp())

    relship = users_cboxes_relation.insert().values(
        user=user.id,
        cashbox_id=cbox["id"],
        token=rel_token,
        status=True,
        is_owner=False,
        created_at=created,
        updated_at=updated,
    )

    rl_id = await database.execute(relship)

    query = users_cboxes_relation.select().where(users_cboxes_relation.c.id == rl_id)
    rl = await database.fetch_one(query)

    q = users.select().where(users.c.id == user.id)
    user = await database.fetch_one(q)

    user_dict = {
        "id": user.id,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "username": user.username,
        "status": user.status,
        "photo": user.photo,
        "is_admin": user.is_admin,
    }

    cbox_id = rl.cashbox_id

    query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.cashbox_id == cbox_id
    )
    all_rl = await database.fetch_all(query)

    tokens_list = [rel.token for rel in all_rl]

    try:
        async with websockets.connect(f"wss://app.tablecrm.com/ws/{rel_token}") as ws:
            await ws.send(
                json.dumps(
                    {
                        "super_secret_token": "143a2854998b0c3ab1e0f38b5a66d12024cd088b9eac8ae39df6161313d254fd",
                        "tokens_list": tokens_list,
                        "user": {"action": "users_create", "result": user_dict},
                    }
                )
            )

            await ws.close()
    except Exception as error:
        print(f"Exception when send websocket add new user: {error}")

    return rl
