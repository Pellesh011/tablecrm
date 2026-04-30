import asyncio
from datetime import datetime

import aiohttp
from database.db import (
    amo_install,
    amo_install_table_cashboxes,
    amo_settings,
    cboxes,
    database,
)
from functions.helpers import gen_token
from ws_manager import manager


async def update_amo_install(amo_post_json, ref, install, code):
    amo_db_data = dict(install)
    async with aiohttp.ClientSession() as session1:
        async with session1.post(
            f"https://{ref}/oauth2/access_token", json=amo_post_json
        ) as resp:
            amo_resp_json1 = await resp.json()

    amo_token = amo_resp_json1.get("access_token")

    if amo_token:
        if not install.field_id:
            headers = {"Authorization": f"Bearer {amo_token}"}
            async with aiohttp.ClientSession(headers=headers) as session:
                field_id = None
                async with session.get(
                    f"https://{ref}/api/v4/contacts/custom_fields"
                ) as resp3:
                    amo_resp_json3 = await resp3.json()
                    print(amo_resp_json3)
                    if amo_resp_json3.get("_embedded"):
                        _emb = amo_resp_json3.get("_embedded")
                        if _emb.get("custom_fields"):
                            for custom_field in amo_resp_json3["_embedded"][
                                "custom_fields"
                            ]:
                                if custom_field["name"] == "Телефон":
                                    field_id = int(custom_field["id"])
                amo_db_data["field_id"] = field_id

    timestamp = int(datetime.utcnow().timestamp())

    amo_db_data["code"] = code
    amo_db_data["access_token"] = amo_resp_json1["access_token"]
    amo_db_data["refresh_token"] = amo_resp_json1["refresh_token"]
    amo_db_data["active"] = True
    amo_db_data["updated_at"] = timestamp

    query = (
        amo_install.update().where(amo_install.c.referrer == ref).values(amo_db_data)
    )
    await database.execute(query)

    integration_dict = {"status": True, "updated_at": timestamp}

    query = (
        amo_install_table_cashboxes.update()
        .where(amo_install_table_cashboxes.c.amo_install_group_id == install["id"])
        .values(integration_dict)
    )
    await database.execute(query=query)

    query = amo_install_table_cashboxes.select().where(
        amo_install_table_cashboxes.c.amo_install_group_id == install["id"]
    )
    cashbox_id = await database.fetch_one(query=query)

    if cashbox_id:
        query = cboxes.select().where(cboxes.c.id == cashbox_id["cashbox_id"])
        cashbox = await database.fetch_one(query=query)

        # await manager.send_message(cashbox["token"], {"result": "paired", "integration_status": True})
        await manager.send_message(
            cashbox.token,
            {"action": "paired", "target": "integrations", "integration_status": True},
        )

    return amo_db_data


async def add_amo_install(amo_post_json, ref, platform, setting_info_id):
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"https://{ref}/oauth2/access_token", json=amo_post_json
        ) as resp:
            amo_resp_json1 = await resp.json()

    amo_token = amo_resp_json1.get("access_token")

    field_id = None

    if amo_token:
        headers = {"Authorization": f"Bearer {amo_token}"}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(f"https://{ref}/api/v4/account") as resp:
                amo_resp_json2 = await resp.json()
            async with session.get(
                f"https://{ref}/api/v4/contacts/custom_fields"
            ) as resp3:
                amo_resp_json3 = await resp3.json()
                print(amo_resp_json3)
                if amo_resp_json3.get("_embedded"):
                    _emb = amo_resp_json3.get("_embedded")
                    if _emb.get("custom_fields"):
                        for custom_field in amo_resp_json3["_embedded"][
                            "custom_fields"
                        ]:
                            if custom_field["name"] == "Телефон":
                                field_id = int(custom_field["id"])

    amo_data = amo_post_json
    time = int(datetime.utcnow().timestamp())
    amo_data.pop("grant_type")
    amo_data.pop("redirect_uri")
    amo_data["referrer"] = ref
    amo_data["amo_account_id"] = int(amo_resp_json2["id"])
    amo_data["active"] = True
    amo_data["platform"] = platform
    amo_data["created_at"] = time
    amo_data["updated_at"] = time
    amo_data["pair_token"] = gen_token()
    amo_data["from_widget"] = setting_info_id
    amo_data["expires_in"] = int(amo_resp_json1["expires_in"])
    amo_data["refresh_token"] = amo_resp_json1["refresh_token"]
    amo_data["access_token"] = amo_token
    amo_data["field_id"] = field_id

    query = amo_install.insert().values(amo_data).returning(amo_install.c.id)
    amo_install_record = await database.fetch_one(query)

    print(f"Successful install amo №{amo_install_record.id}")

    return {
        "amo_install_id": amo_install_record.id,
        "expires_in": amo_data["expires_in"],
    }


async def refresh_token(referer):
    query = amo_install.select().where(amo_install.c.referrer == referer)
    amo_db_referers = await database.fetch_all(query=query)

    for amo_db_referer in amo_db_referers:
        if amo_db_referer.active:
            query = amo_settings.select().where(
                amo_settings.c.id == amo_db_referer.from_widget
            )
            setting_info = await database.fetch_one(query)

            amo_post_json = {
                "client_id": amo_db_referer.client_id,
                "client_secret": setting_info.client_secret,
                "grant_type": "refresh_token",
                "refresh_token": amo_db_referer.refresh_token,
                "redirect_uri": setting_info.redirect_uri,
            }

            # q = amo_install_table_cashboxes.select().where(amo_install_table_cashboxes.c.amo_install_group_id == amo_db_referer.id)
            # amo_tablecrm_rel = await database.fetch_one(query)
            #
            # q = cboxes.select().where(cboxes.c.id == amo_tablecrm_rel.cashbox_id)
            # cashbox = await database.fetch_one(query)

            async with aiohttp.ClientSession() as session1:
                async with session1.post(
                    f"https://{referer}/oauth2/access_token", json=amo_post_json
                ) as resp:
                    amo_resp_json = await resp.json()
                    # event_body = {
                    #     "type": "amoevent",
                    #     "name": "Обновление Access токена",
                    #     "url": resp.url,
                    #     "payload": amo_post_json,
                    #     "cashbox_id": cashbox.id,
                    #     "user_id": cashbox.admin,
                    #     "token": None,
                    #     "ip": "https://app.tablecrm.com/",
                    #     "promoimage": None,
                    #     "promodata": None,
                    #     "method": "POST",
                    # }
                    # await database.execute(
                    #     events.insert().values(event_body)
                    # )
            amo_db_referer_dict = dict(amo_db_referer)
            amo_db_referer_dict["access_token"] = amo_resp_json["access_token"]
            amo_db_referer_dict["refresh_token"] = amo_resp_json["refresh_token"]

            query = (
                amo_install.update()
                .where(amo_install.c.id == amo_db_referer.id)
                .values(amo_db_referer_dict)
            )
            await database.execute(query)
            await asyncio.sleep(0.5)
            # return amo_resp_json['access_token']
