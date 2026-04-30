import json

import aiohttp
from database.db import (
    TypeCustomField,
    amo_custom_fields,
    amo_install_custom_fields,
    database,
)
from sqlalchemy import or_


async def get_amo_codes_leads(cashbox_id: int):
    query = (
        amo_custom_fields.select()
        .outerjoin(
            amo_install_custom_fields,
            amo_custom_fields.c.id == amo_install_custom_fields.c.custom_field_id,
        )
        .where(
            or_(
                amo_install_custom_fields.c.cashbox_id == cashbox_id,
                amo_install_custom_fields.c.id.is_(None),
            )
        )
        .where(amo_custom_fields.c.type_entity == TypeCustomField.Lead)
    )
    amo_custom_fields_list = await database.fetch_all(query)
    return amo_custom_fields_list


async def get_amo_codes_contacts(cashbox_id: int):
    query = (
        amo_custom_fields.select()
        .outerjoin(
            amo_install_custom_fields,
            amo_custom_fields.c.id == amo_install_custom_fields.c.custom_field_id,
        )
        .where(
            or_(
                amo_install_custom_fields.c.cashbox_id == cashbox_id,
                amo_install_custom_fields.c.id.is_(None),
            )
        )
        .where(amo_custom_fields.c.type_entity == TypeCustomField.Contact)
    )
    amo_custom_fields_list = await database.fetch_all(query)
    return amo_custom_fields_list


async def create_amo_group_contacts(referer: str, access_token: str):
    headers = {"Authorization": f"Bearer {access_token}"}
    group_id = None

    async with aiohttp.ClientSession(headers=headers) as http_session:
        url = f"https://{referer}/api/v4/contacts/custom_fields/groups"
        async with http_session.get(url) as groups_resp:
            groups_resp.raise_for_status()
            resp_json = await groups_resp.json()
            if "_embedded" in resp_json:
                if "custom_field_groups" in resp_json["_embedded"]:
                    for group in resp_json["_embedded"]["custom_field_groups"]:
                        if group["name"] == "Tablecrm":
                            group_id = group["id"]

        if not group_id:
            data = [{"name": "Tablecrm", "sort": 2}]
            async with http_session.post(url, data=json.dumps(data)) as groups_resp:
                groups_resp.raise_for_status()
                resp_json = await groups_resp.json()
                if "_embedded" in resp_json:
                    if "custom_field_groups" in resp_json["_embedded"]:
                        for group in resp_json["_embedded"]["custom_field_groups"]:
                            if group["name"] == "Tablecrm":
                                group_id = group["id"]
    return group_id


async def create_amo_group_leads(referer: str, access_token: str):
    headers = {"Authorization": f"Bearer {access_token}"}
    group_id = None

    async with aiohttp.ClientSession(headers=headers) as http_session:
        url = f"https://{referer}/api/v4/leads/custom_fields/groups"

        async with http_session.get(url) as groups_resp:
            groups_resp.raise_for_status()
            resp_json = await groups_resp.json()
            if "_embedded" in resp_json:
                if "custom_field_groups" in resp_json["_embedded"]:
                    for group in resp_json["_embedded"]["custom_field_groups"]:
                        if group["name"] == "Tablecrm":
                            group_id = group["id"]

        if not group_id:
            data = [{"name": "Tablecrm", "sort": 2}]
            async with http_session.post(url, data=json.dumps(data)) as groups_resp:
                groups_resp.raise_for_status()
                resp_json = await groups_resp.json()
                if "_embedded" in resp_json:
                    if "custom_field_groups" in resp_json["_embedded"]:
                        for group in resp_json["_embedded"]["custom_field_groups"]:
                            if group["name"] == "Tablecrm":
                                group_id = group["id"]
    return group_id


async def post_custom_fields_leads(fields_predata, referer: str, access_token: str):
    field_ids = []
    headers = {"Authorization": f"Bearer {access_token}"}
    async with aiohttp.ClientSession(headers=headers) as http_session:
        url = f"https://{referer}/api/v4/leads/custom_fields"
        request_json = json.dumps(fields_predata)
        async with http_session.post(url, data=request_json) as groups_resp:
            print(await groups_resp.text())
            groups_resp.raise_for_status()
            resp_json = await groups_resp.json()
            if "_embedded" in resp_json:
                if "custom_fields" in resp_json["_embedded"]:
                    for y in resp_json["_embedded"]["custom_fields"]:
                        field_ids.append(y["id"])


async def post_custom_fields_contacts(fields_predata, referer: str, access_token: str):
    field_ids = []
    headers = {"Authorization": f"Bearer {access_token}"}
    async with aiohttp.ClientSession(headers=headers) as http_session:
        url = f"https://{referer}/api/v4/contacts/custom_fields"
        request_json = json.dumps(fields_predata)
        async with http_session.post(url, data=request_json) as groups_resp:
            print(await groups_resp.text())
            groups_resp.raise_for_status()
            resp_json = await groups_resp.json()
            if "_embedded" in resp_json:
                if "custom_fields" in resp_json["_embedded"]:
                    for y in resp_json["_embedded"]["custom_fields"]:
                        field_ids.append(y["id"])


async def create_custom_fields_leads(cashbox_id: int, referer: str, access_token: str):
    amo_custom_fields_list = await get_amo_codes_leads(cashbox_id=cashbox_id)
    amo_codes = await get_custom_fields_codes_leads(
        referer=referer, access_token=access_token
    )
    must_create_fields = []
    for amo_custom_field in amo_custom_fields_list:
        if amo_custom_field.code not in amo_codes:
            must_create_fields.append(amo_custom_field)
    if must_create_fields:
        group_id = await create_amo_group_leads(
            referer=referer, access_token=access_token
        )
        if group_id:
            fields_predata = []
            for amo_custom_field in must_create_fields:
                fields_predata.append(
                    {
                        "code": amo_custom_field.code,
                        "name": amo_custom_field.name,
                        "type": amo_custom_field.type,
                        "group_id": group_id,
                    }
                )
            if fields_predata:
                field_ids = await post_custom_fields_leads(
                    fields_predata=fields_predata,
                    referer=referer,
                    access_token=access_token,
                )
                if field_ids:
                    print(f"КАСТОМНЫЕ ПОЛЯ AMO {referer} УСПЕШНО СОЗДАНЫ")
        else:
            print(f"ПРИ СОЗДАНИИ КАСТОМНЫХ ПОЛЕЙ AMO {referer} ПРОИЗОШЛА ОШИБКА")


async def create_custom_fields_contacts(
    cashbox_id: int, referer: str, access_token: str
):
    amo_custom_fields_list = await get_amo_codes_contacts(cashbox_id=cashbox_id)
    amo_codes = await get_custom_fields_codes_contacts(
        referer=referer, access_token=access_token
    )
    must_create_fields = []
    for amo_custom_field in amo_custom_fields_list:
        if amo_custom_field.code not in amo_codes:
            must_create_fields.append(amo_custom_field)
    if must_create_fields:
        group_id = await create_amo_group_contacts(
            referer=referer, access_token=access_token
        )
        if group_id:
            fields_predata = []
            for amo_custom_field in must_create_fields:
                fields_predata.append(
                    {
                        "code": amo_custom_field.code,
                        "name": amo_custom_field.name,
                        "type": amo_custom_field.type,
                        "group_id": group_id,
                    }
                )
            if fields_predata:
                field_ids = await post_custom_fields_contacts(
                    fields_predata=fields_predata,
                    referer=referer,
                    access_token=access_token,
                )
                if field_ids:
                    print(f"КАСТОМНЫЕ ПОЛЯ AMO {referer} УСПЕШНО СОЗДАНЫ")
        else:
            print(f"ПРИ СОЗДАНИИ КАСТОМНЫХ ПОЛЕЙ AMO {referer} ПРОИЗОШЛА ОШИБКА")


async def get_custom_fields_codes_leads(referer: str, access_token: str):
    codes = []
    custom_fields_url = f"https://{referer}/api/v4/leads/custom_fields"
    headers = {"Authorization": f"Bearer {access_token}"}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(custom_fields_url) as response:
            response.raise_for_status()
            data = await response.json()
            if "_embedded" in data:
                if "custom_fields" in data["_embedded"]:
                    codes = [x["code"] for x in data["_embedded"]["custom_fields"]]
    return codes


async def get_custom_fields_codes_contacts(referer: str, access_token: str):
    codes = []
    custom_fields_url = f"https://{referer}/api/v4/contacts/custom_fields"
    headers = {"Authorization": f"Bearer {access_token}"}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(custom_fields_url) as response:
            response.raise_for_status()
            data = await response.json()
            if "_embedded" in data:
                if "custom_fields" in data["_embedded"]:
                    codes = [x["code"] for x in data["_embedded"]["custom_fields"]]
    return codes
