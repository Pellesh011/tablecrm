from datetime import datetime

from apps.amocrm.pair.functions.fields import (
    create_custom_fields_contacts,
    create_custom_fields_leads,
)
from database.db import (
    amo_install,
    amo_install_groups,
    amo_install_table_cashboxes,
    database,
    users_cboxes_relation,
)
from fastapi import APIRouter, HTTPException
from functions.helpers import gen_token
from ws_manager import manager

router = APIRouter(tags=["amocrm"])


@router.get("/integration_pair/")
async def sc_l(token: str, amo_token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:
        query = amo_install_groups.select().where(
            amo_install_groups.c.pair_token == amo_token
        )
        a_t_group = await database.fetch_one(query=query)

        if a_t_group:
            query = amo_install_table_cashboxes.select().where(
                amo_install_table_cashboxes.c.amo_install_group_id == a_t_group.id
            )
            amo_pair = await database.fetch_one(query=query)

            time = int(datetime.utcnow().timestamp())
            integration_data = {}

            query = amo_install.select().where(
                amo_install.c.install_group_id == a_t_group.id
            )
            amo_installs_in_group = await database.fetch_all(query)

            active_amo_install = None
            flag = False
            for install_info in amo_installs_in_group:
                if install_info.active:
                    active_amo_install = install_info
                    flag = True

            if flag and active_amo_install:
                await create_custom_fields_contacts(
                    cashbox_id=user["cashbox_id"],
                    referer=active_amo_install.referrer,
                    access_token=active_amo_install.access_token,
                )

                await create_custom_fields_leads(
                    cashbox_id=user["cashbox_id"],
                    referer=active_amo_install.referrer,
                    access_token=active_amo_install.access_token,
                )

            if not amo_pair:
                integration_data["cashbox_id"] = user["cashbox_id"]
                integration_data["amo_install_group_id"] = a_t_group.id
                integration_data["last_token"] = amo_token
                integration_data["status"] = flag
                integration_data["created_at"] = time
                integration_data["updated_at"] = time

                query = amo_install_table_cashboxes.insert().values(integration_data)
                await database.execute(query)

            else:
                integration_data["last_token"] = amo_token
                integration_data["updated_at"] = time

                query = (
                    amo_install_table_cashboxes.update()
                    .where(
                        amo_install_table_cashboxes.c.amo_install_group_id
                        == a_t_group.id
                    )
                    .values(integration_data)
                )
                await database.execute(query)

            await manager.send_message(
                user.token,
                {
                    "action": "paired",
                    "target": "integrations",
                    "integration_status": True,
                },
            )

            return {"status": "success"}

        else:
            raise HTTPException(
                status_code=403, detail="Вы ввели некорректный токен амо!"
            )

    else:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/get_my_token/")
async def sc_l(referer: str):
    query = amo_install_groups.select().where(amo_install_groups.c.referrer == referer)
    a_t_group = await database.fetch_one(query=query)

    if a_t_group:
        return {"token": a_t_group.pair_token}
    else:
        return {"status": "incorrect token!"}


@router.get("/refresh_my_token/")
async def sc_l(referer: str):
    query = amo_install_groups.select().where(amo_install_groups.c.referrer == referer)
    a_t_group = await database.fetch_one(query=query)
    if a_t_group:

        new_token = gen_token()

        query = (
            amo_install_groups.select()
            .where(amo_install_groups.c.referrer == referer)
            .values({"pair_token": new_token})
        )
        await database.execute(query)

        query = amo_install_table_cashboxes.select().where(
            amo_install_table_cashboxes.c.amo_install_group_id == a_t_group.id
        )
        pair = await database.fetch_one(query)

        query = users_cboxes_relation.select().where(
            users_cboxes_relation.c.cashbox_id == pair["cashbox_id"]
        )
        cashbox = await database.fetch_one(query)

        await manager.send_message(
            cashbox.token,
            {
                "action": "paired",
                "target": "integrations",
                "integration_status": "need_to_refresh",
            },
        )

        return {"token": new_token}
    else:
        return {"status": "incorrect token!"}


@router.get("/check_pair/")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:

        query = amo_install_table_cashboxes.select().where(
            amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"]
        )
        pair = await database.fetch_one(query=query)

        if pair:
            query = amo_install_groups.select().where(
                amo_install_groups.c.id == pair["amo_install_group_id"]
            )
            a_t_group = await database.fetch_one(query=query)

            if pair["last_token"] != a_t_group["pair_token"]:
                return {"result": "paired", "integration_status": "need_to_refresh"}
            else:
                return {"result": "paired", "integration_status": pair["status"]}
        else:
            return {"result": "not paired"}
    else:
        return {"status": "incorrect token!"}


@router.get("/integration_unpair/")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        query = (
            amo_install_table_cashboxes.update()
            .where(amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"])
            .values(status=False, updated_at=int(datetime.utcnow().timestamp()))
        )
        await database.execute(query)

        await manager.send_message(
            user.token,
            {"action": "paired", "target": "integrations", "integration_status": False},
        )

    else:
        return {"status": "incorrect token!"}


@router.get("/integration_on/")
async def sc_l(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query=query)
    if user:
        query = amo_install_table_cashboxes.select().where(
            amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"]
        )
        pair = await database.fetch_one(query=query)

        query = amo_install.select().where(
            amo_install.c.install_group_id == pair.amo_install_group_id
        )
        amo_installs_in_group = await database.fetch_all(query)

        flag = False
        for install_info in amo_installs_in_group:
            if install_info.active:
                flag = True

        if not flag:
            return {"status": "Your amo services is not active"}

        query = (
            amo_install_table_cashboxes.update()
            .where(amo_install_table_cashboxes.c.cashbox_id == user["cashbox_id"])
            .values(
                status=True,
                updated_at=int(datetime.utcnow().timestamp()),
            )
        )
        await database.fetch_one(query=query)

        await manager.send_message(
            user.token,
            {"action": "paired", "target": "integrations", "integration_status": True},
        )
    else:
        return {"status": "incorrect token!"}
