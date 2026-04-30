from datetime import datetime

import aiohttp
from apps.tochka_bank.schemas import AccountUpdate
from database.db import (
    database,
    integrations,
    integrations_to_cashbox,
    pboxes,
    tochka_bank_accounts,
    tochka_bank_credentials,
    users_cboxes_relation,
)

# from jobs.jobs import scheduler
from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse
from functions.helpers import get_user_by_token
from jobs.tochka_bank_job.job import tochka_update_transaction
from sqlalchemy import and_, select
from ws_manager import manager

router = APIRouter(tags=["Tochka bank"])


async def integration_info(cashbox_id, id_integration):
    query = (
        select(
            integrations_to_cashbox.c.installed_by,
            users_cboxes_relation.c.token,
            integrations_to_cashbox.c.id,
            *integrations.columns,
        )
        .where(users_cboxes_relation.c.cashbox_id == cashbox_id)
        .select_from(users_cboxes_relation)
        .join(
            integrations_to_cashbox,
            users_cboxes_relation.c.id == integrations_to_cashbox.c.installed_by,
        )
        .select_from(integrations_to_cashbox)
        .join(
            integrations, integrations.c.id == integrations_to_cashbox.c.integration_id
        )
        .where(integrations.c.id == id_integration)
    )
    return await database.fetch_one(query)


@router.post("/bank/refresh_token")
async def refresh_token(integration_cashboxes: int):
    integration_cbox = await database.fetch_one(
        integrations_to_cashbox.select().where(
            integrations_to_cashbox.c.id == integration_cashboxes
        )
    )
    integration = await database.fetch_one(
        integrations.select().where(
            integrations.c.id == integration_cbox.get("integration_id")
        )
    )
    credentials = await database.fetch_one(
        tochka_bank_credentials.select().where(
            tochka_bank_credentials.c.integration_cashboxes == integration_cashboxes
        )
    )
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(
            "https://enter.tochka.com/connect/token",
            data={
                "client_id": integration.get("client_app_id"),
                "client_secret": integration.get("client_secret"),
                "grant_type": "refresh_token",
                "refresh_token": credentials.get("refresh_token"),
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as resp:
            token_json = await resp.json()
        await session.close()
    try:
        await database.execute(
            tochka_bank_credentials.update()
            .where(
                tochka_bank_credentials.c.integration_cashboxes == integration_cashboxes
            )
            .values(
                {
                    "access_token": token_json.get("access_token"),
                    "refresh_token": token_json.get("refresh_token"),
                }
            )
        )
        return {"result": token_json}
    except:
        raise HTTPException(status_code=422, detail="ошибка обновления токена")


@router.get("/bank/tochkaoauth")
async def tochkaoauth(code: str, state: int):
    """Hook для oauth банка"""

    user_integration = await integration_info(state, 1)

    if not user_integration:
        raise HTTPException(status_code=432, detail="user not found with integration")

    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(
            "https://enter.tochka.com/connect/token",
            data={
                "client_id": user_integration.get("client_app_id"),
                "client_secret": user_integration.get("client_secret"),
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": user_integration.get("redirect_uri"),
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as resp:
            token_json = await resp.json()
        await session.close()
    if token_json.get("error"):
        raise HTTPException(
            status_code=432, detail=f"error OAuth2 {token_json.get('error')}"
        )
    try:
        credential = await database.fetch_one(
            tochka_bank_credentials.select(
                tochka_bank_credentials.c.integration_cashboxes
                == user_integration.get("id")
            )
        )
        if not credential:
            credentials_id = await database.execute(
                tochka_bank_credentials.insert().values(
                    {
                        "access_token": token_json.get("access_token"),
                        "refresh_token": token_json.get("refresh_token"),
                        "integration_cashboxes": user_integration.get("id"),
                    }
                )
            )
        else:
            await database.execute(
                tochka_bank_credentials.update()
                .where(
                    tochka_bank_credentials.c.integration_cashboxes
                    == user_integration.get("id")
                )
                .values(
                    {
                        "access_token": token_json.get("access_token"),
                        "refresh_token": token_json.get("refresh_token"),
                    }
                )
            )
            credentials_id = credential.get("id")
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))

    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get(
            "https://enter.tochka.com/uapi/open-banking/v1.0/accounts",
            headers={
                "Authorization": f'Bearer {token_json.get("access_token")}',
                "Content-type": "application/json",
            },
        ) as resp:
            accounts_json = await resp.json()
        await session.close()
    if len(accounts_json.get("Data").get("Account")) > 0:
        for account in accounts_json.get("Data").get("Account"):
            async with aiohttp.ClientSession(trust_env=True) as session:
                async with session.get(
                    f'https://enter.tochka.com/uapi/open-banking/v1.0/accounts/{account.get("accountId")}/balances',
                    headers={
                        "Authorization": f'Bearer {token_json.get("access_token")}',
                        "Content-type": "application/json",
                    },
                ) as resp:
                    balance_json = await resp.json()
                await session.close()
            created_date = datetime.utcnow().date()
            created_date_ts = int(
                datetime.timestamp(datetime.combine(created_date, datetime.min.time()))
            )
            data = {
                "name": f"Счет банк Точка №{account.get('accountId').split('/')[0]}",
                "start_balance": 0,
                "cashbox": state,
                "balance": balance_json.get("Data")
                .get("Balance")[0]
                .get("Amount")
                .get("amount"),
                "update_start_balance": int(datetime.utcnow().timestamp()),
                "update_start_balance_date": int(datetime.utcnow().timestamp()),
                "created_at": int(datetime.utcnow().timestamp()),
                "updated_at": int(datetime.utcnow().timestamp()),
                "balance_date": 0,
            }
            account_db = await database.fetch_one(
                tochka_bank_accounts.select().where(
                    tochka_bank_accounts.c.accountId == account.get("accountId")
                )
            )
            if not account_db:
                id_paybox = await database.execute(pboxes.insert().values(data))
                await database.execute(
                    tochka_bank_accounts.insert().values(
                        {
                            "payboxes_id": id_paybox,
                            "tochka_bank_credential_id": credentials_id,
                            "customerCode": account.get("customerCode"),
                            "accountId": account.get("accountId"),
                            "transitAccount": account.get("transitAccount"),
                            "status": account.get("status"),
                            "statusUpdateDateTime": account.get("statusUpdateDateTime"),
                            "currency": account.get("currency"),
                            "accountType": account.get("accountType"),
                            "accountSubType": account.get("accountSubType"),
                            "registrationDate": account.get("registrationDate"),
                            "is_deleted": False,
                            "is_active": False,
                        }
                    )
                )
            else:
                del data["created_at"]
                id_paybox = await database.execute(
                    pboxes.update()
                    .where(pboxes.c.id == account_db.get("payboxes_id"))
                    .values(data)
                )
                await database.execute(
                    tochka_bank_accounts.update()
                    .where(tochka_bank_accounts.c.id == account_db.get("id"))
                    .values(
                        {
                            "payboxes_id": account_db.get("payboxes_id"),
                            "tochka_bank_credential_id": credentials_id,
                            "customerCode": account.get("customerCode"),
                            "accountId": account.get("accountId"),
                            "transitAccount": account.get("transitAccount"),
                            "status": account.get("status"),
                            "statusUpdateDateTime": account.get("statusUpdateDateTime"),
                            "currency": account.get("currency"),
                            "accountType": account.get("accountType"),
                            "accountSubType": account.get("accountSubType"),
                            "registrationDate": account.get("registrationDate"),
                            "is_deleted": False,
                            "is_active": False,
                        }
                    )
                )

    # if not scheduler.get_job(job_id = str(user_integration.get('installed_by'))):
    #     scheduler.add_job(refresh_token, 'interval', seconds = int(token_json.get('expires_in')), kwargs = {'integration_cashboxes': user_integration.get('id')}, name = 'refresh token', id = str(user_integration.get('installed_by')))
    # else:
    #     scheduler.get_job(job_id = str(user_integration.get('installed_by'))).reschedule('interval', seconds = int(token_json.get('expires_in')))
    return RedirectResponse(
        f'https://app.tablecrm.com/integrations?token={user_integration.get("token")}'
    )


@router.get("/bank/get_oauth_link/")
async def get_token_for_scope(token: str, id_integration: int):
    """Получение токена для работы с разрешениями"""

    user = await get_user_by_token(token)
    user_integration = await integration_info(user.get("cashbox_id"), id_integration)

    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(
            "https://enter.tochka.com/connect/token",
            data={
                "client_id": user_integration.get("client_app_id"),
                "client_secret": user_integration.get("client_secret"),
                "grant_type": "client_credentials",
                "scope": user_integration.get("scopes"),
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as resp:
            token_scope_json = await resp.json()
        await session.close()

    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(
            "https://enter.tochka.com/uapi/v1.0/consents",
            json={
                "Data": {
                    "permissions": [
                        "ReadAccountsBasic",
                        "ReadAccountsDetail",
                        "MakeAcquiringOperation",
                        "ReadAcquiringData",
                        "ReadBalances",
                        "ReadStatements",
                        "ReadCustomerData",
                        "ReadSBPData",
                        "EditSBPData",
                        "CreatePaymentForSign",
                        "CreatePaymentOrder",
                        "ManageWebhookData",
                        "ManageInvoiceData",
                    ]
                }
            },
            headers={
                "Authorization": f'Bearer {token_scope_json.get("access_token")}',
                "Content-Type": "application/json",
            },
        ) as resp:
            api_resp_json = await resp.json()
        await session.close()

        link = (
            f'{user_integration.get( "url" )}authorize?'
            f'client_id={api_resp_json.get("Data").get("clientId")}&'
            f"response_type=code&"
            f'redirect_uri={user_integration.get("redirect_uri")}&'
            f'consent_id={api_resp_json.get("Data").get("consentId")}&'
            f'scope={user_integration.get("scopes")}&'
            f'state={user.get("cashbox_id")}'
        )

    return {"link": link}


@router.get("/bank/check")
async def check(token: str, id_integration: int):
    """Проверка установлена или нет интеграция у клиента"""

    user = await get_user_by_token(token)

    check = await database.fetch_one(
        integrations_to_cashbox.select().where(
            and_(
                integrations_to_cashbox.c.integration_id == id_integration,
                integrations_to_cashbox.c.installed_by == user.id,
            )
        )
    )
    if check is None:
        raise HTTPException(
            status_code=204, detail="integration not installed by chashbox"
        )

    message = {
        "action": "check",
        "target": "IntegrationTochkaBank",
        "integration_status": check.get("status"),
    }

    isAuth = await database.fetch_one(
        tochka_bank_credentials.select().where(
            tochka_bank_credentials.c.integration_cashboxes == check.get("id")
        )
    )

    if isAuth:
        message.update({"integration_isAuth": True})
    else:
        message.update({"integration_isAuth": False})
    await manager.send_message(user.token, message)
    return {"isAuth": message.get("integration_isAuth")}


@router.get("/bank/integration_on")
async def integration_on(token: str, id_integration: int):
    """Установка связи аккаунта пользователя и интеграции"""

    user = await get_user_by_token(token)

    try:
        check = await database.fetch_one(
            integrations_to_cashbox.select().where(
                and_(
                    integrations_to_cashbox.c.integration_id == id_integration,
                    integrations_to_cashbox.c.installed_by == user.id,
                )
            )
        )
        if check:
            await database.execute(
                integrations_to_cashbox.update()
                .where(
                    and_(
                        integrations_to_cashbox.c.integration_id == id_integration,
                        integrations_to_cashbox.c.installed_by == user.id,
                    )
                )
                .values({"status": True})
            )
            refresh = await refresh_token(check.get("id"))
            if not refresh:
                raise HTTPException(
                    status_code=422, detail="ошибка обновления ключа доступа"
                )
        else:
            await database.execute(
                integrations_to_cashbox.insert().values(
                    {
                        "integration_id": id_integration,
                        "installed_by": user.get("id"),
                        "deactivated_by": user.get("id"),
                        "status": True,
                    }
                )
            )
            refresh = False
        # if not scheduler.get_job(job_id = str(check.get('installed_by'))):
        #     scheduler.add_job(refresh_token, 'interval', seconds = 86400,
        #                       kwargs = {'integration_cashboxes': check.get('id')},
        #                       name = 'refresh token', id = str(check.get('installed_by')))
        # else:
        #     scheduler.get_job(job_id = str(check.get('installed_by'))).reschedule('interval', seconds = 86400)

        await manager.send_message(
            user.token,
            {
                "action": "on",
                "target": "IntegrationTochkaBank",
                "integration_status": True,
                "integration_isAuth": True if refresh else False,
            },
        )
        return {"result": "ok"}
    except:
        raise HTTPException(
            status_code=422,
            detail="ошибка установки связи аккаунта пользователя и интеграции",
        )


@router.get("/bank/integration_off")
async def integration_off(token: str, id_integration: int):
    """Удаление связи аккаунта пользователя и интеграции"""

    user = await get_user_by_token(token)
    try:
        await database.execute(
            integrations_to_cashbox.update()
            .where(
                and_(
                    integrations_to_cashbox.c.integration_id == id_integration,
                    integrations_to_cashbox.c.installed_by == user.id,
                )
            )
            .values({"status": False})
        )

        await manager.send_message(
            user.token,
            {
                "action": "off",
                "target": "IntegrationTochkaBank",
                "integration_status": False,
            },
        )

        # if scheduler.get_job(job_id = str(user.get("id"))):
        #     scheduler.remove_job(job_id = str(user.get("id")))
        return {"isAuth": False}
    except:
        raise HTTPException(
            status_code=422,
            detail="ошибка удаления связи аккаунта пользователя и интеграции",
        )


@router.get("/bank/accounts/")
async def accounts(token: str, id_integration: int):
    """Получение списка счетов аккаунта банка"""

    user = await get_user_by_token(token)

    query = (
        select(
            tochka_bank_accounts.c.id,
            pboxes.c.name,
            tochka_bank_accounts.c.currency,
            tochka_bank_accounts.c.accountType,
            tochka_bank_accounts.c.is_active,
        )
        .where(
            and_(
                pboxes.c.cashbox == user.get("cashbox_id"),
                pboxes.c.deleted_at.is_(None),
            )
        )
        .select_from(pboxes)
        .join(tochka_bank_accounts, tochka_bank_accounts.c.payboxes_id == pboxes.c.id)
        .order_by(pboxes.c.name)
    )
    accounts = await database.fetch_all(query)
    return {"result": accounts}


@router.patch("/bank/accounts/update/{idx}/")
async def update_account(token: str, idx: int, account: AccountUpdate):
    """Обновление счета аккаунта банка"""
    try:
        account_data = account.dict(exclude_unset=True)
        await get_user_by_token(token)
        account_db = await database.fetch_one(
            tochka_bank_accounts.select().where(tochka_bank_accounts.c.id == idx)
        )
        account_model = AccountUpdate(**account_db)
        updated_account = account_model.copy(update=account_data)
        await database.execute(
            tochka_bank_accounts.update()
            .where(tochka_bank_accounts.c.id == idx)
            .values(updated_account.dict())
        )
        account_result = await database.fetch_one(
            tochka_bank_accounts.select().where(tochka_bank_accounts.c.id == idx)
        )
        if account_result.get("is_active"):
            await tochka_update_transaction()
        return {"result": account_result}
    except Exception as error:
        raise HTTPException(status_code=432, detail=str(error))
