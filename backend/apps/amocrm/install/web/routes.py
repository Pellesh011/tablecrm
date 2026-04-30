from datetime import datetime

import aiohttp
from apps.amocrm.install.functions.get_account_info import get_account_info
from apps.amocrm.install.infrastructure.impl.AmoCRMAuthenticator import (
    AmoCRMAuthenticator,
)
from apps.amocrm.pair.functions.fields import (
    create_custom_fields_contacts,
    create_custom_fields_leads,
)
from database.db import (
    amo_install,
    amo_install_groups,
    amo_install_table_cashboxes,
    amo_settings,
    database,
    users_cboxes_relation,
)
from fastapi import APIRouter, HTTPException
from functions.helpers import gen_token
from sqlalchemy import and_, select
from starlette import status
from ws_manager import manager

router = APIRouter(tags=["amocrm"])


@router.get("/amo_connect")
async def sc_l(
    code: str, referer: str, platform: int, client_id: str, from_widget: str
):
    query = amo_settings.select().where(amo_settings.c.integration_id == client_id)
    setting_info = await database.fetch_one(query)

    if not setting_info:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="widget not found"
        )

    query = select(amo_install).where(
        and_(
            amo_install.c.referrer == referer,
            amo_install.c.from_widget == setting_info.id,
        )
    )
    install = await database.fetch_one(query)
    install_group = None

    if install:
        install_group = install.install_group_id
    else:
        query = select(amo_install_groups).where(
            amo_install_groups.c.referrer == referer
        )
        install_group_info = await database.fetch_one(query)
        if install_group_info:
            install_group = install_group_info.id

    async with aiohttp.ClientSession() as session:

        amocrm_auth = AmoCRMAuthenticator(
            session,
            client_id,
            setting_info.client_secret,
            setting_info.redirect_uri,
            referer,
        )
        amo_crm_install = await amocrm_auth.authenticate(code)

        account_info = await get_account_info(
            referer=referer, access_token=amo_crm_install.access_token
        )

        values_dict = {
            "code": code,
            "referrer": referer,
            "platform": platform,
            "amo_account_id": account_info["id"],
            "client_id": client_id,
            "client_secret": setting_info.client_secret,
            "refresh_token": amo_crm_install.refresh_token,
            "access_token": amo_crm_install.access_token,
            "expires_in": int(amo_crm_install.expires_in),
            "active": True,
            "from_widget": setting_info.id,
        }
        if not install_group:
            query = (
                amo_install_groups.insert()
                .values(
                    referrer=referer,
                    pair_token=gen_token(),
                )
                .returning(amo_install_groups.c.id)
            )
            amo_install_group_return = await database.fetch_one(query)
            amo_install_group = amo_install_group_return.id
            values_dict["install_group_id"] = amo_install_group

        if install:
            query = amo_install.update().where(
                and_(
                    amo_install.c.referrer == referer,
                    amo_install.c.from_widget == setting_info.id,
                )
            )
        else:
            query = amo_install.insert()
        query = query.values(values_dict)
        await database.execute(query)

        if install:
            query = amo_install_table_cashboxes.select().where(
                amo_install_table_cashboxes.c.amo_install_group_id == install_group
            )
        else:
            query = amo_install_table_cashboxes.select().where(
                amo_install_table_cashboxes.c.amo_install_group_id == amo_install_group
            )
        pair_info = await database.fetch_one(query)
        if pair_info:
            await create_custom_fields_contacts(
                cashbox_id=pair_info.cashbox_id,
                referer=referer,
                access_token=amo_crm_install.access_token,
            )

            await create_custom_fields_leads(
                cashbox_id=pair_info.cashbox_id,
                referer=referer,
                access_token=amo_crm_install.access_token,
            )

            query = (
                amo_install_table_cashboxes.update()
                .where(
                    amo_install_table_cashboxes.c.amo_install_group_id == install_group
                )
                .values({"status": True})
            )
            await database.execute(query)


@router.get("/amo_disconnect")
async def sc_l(account_id: int, client_uuid: str):
    print("Отключение виджета")
    query = amo_install.select().where(
        amo_install.c.amo_account_id == account_id
        and amo_install.c.client_id == client_uuid
    )
    a_t = await database.fetch_one(query)

    print(a_t)

    if not a_t:
        return {"result": "amo token does not connected!"}

    query = (
        amo_install.update().where(amo_install.c.id == a_t.id).values({"active": False})
    )
    await database.execute(query)

    query = amo_install.select().where(
        amo_install.c.install_group_id == a_t.install_group_id
    )
    amo_installs_in_group = await database.fetch_all(query)

    flag = False
    for install_info in amo_installs_in_group:
        if install_info.active:
            flag = True

    if not flag:
        query = (
            amo_install_table_cashboxes.update()
            .where(
                amo_install_table_cashboxes.c.amo_install_group_id
                == a_t.install_group_id
            )
            .values({"status": False, "updated_at": int(datetime.utcnow().timestamp())})
        )
        await database.execute(query)

        query = amo_install_table_cashboxes.select().where(
            amo_install_table_cashboxes.c.amo_install_group_id == a_t.install_group_id
        )
        relship = await database.fetch_one(query)

        if relship:
            query = users_cboxes_relation.select().where(
                users_cboxes_relation.c.cashbox_id == relship.cashbox_id
            )
            cashboxes = await database.fetch_all(query)

            for cashbox in cashboxes:
                await manager.send_message(
                    cashbox.token,
                    {
                        "action": "paired",
                        "target": "integrations",
                        "integration_status": False,
                    },
                )

    return {"status": "amo token disconnected succesfully!"}
