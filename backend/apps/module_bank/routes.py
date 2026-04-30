import aiohttp
from apps.tochka_bank.routes import integration_info
from database.db import (
    database,
    integrations_to_cashbox,
    module_bank_accounts,
    module_bank_credentials,
    pboxes,
)
from fastapi import APIRouter, HTTPException
from functions.helpers import get_user_by_token
from sqlalchemy import and_, select
from starlette.responses import RedirectResponse
from ws_manager import manager

router = APIRouter(tags=["Module bank"])


@router.get("/bank/moduloauth")
async def moduloauth(code: str, state: int):
    """Hook для oauth банка"""

    user_integration = await integration_info(state, 3)
    if not user_integration:
        raise HTTPException(status_code=432, detail="user not found with integration")

    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(
            "https://api.modulbank.ru/v1/oauth/token",
            json={
                "code": str(code),
                "clientId": str(user_integration.get("client_app_id")),
                "clientSecret": str(user_integration.get("client_secret")),
            },
            headers={"Content-type": "application/json"},
        ) as resp:
            token_json = await resp.json()
            print(token_json)
    if token_json.get("error"):
        raise HTTPException(
            status_code=432, detail=f"error OAuth2 {token_json.get('error')}"
        )
    try:
        credential = await database.fetch_one(
            module_bank_credentials.select(
                module_bank_credentials.c.integration_cashboxes
                == user_integration.get("id")
            )
        )
        if not credential:
            await database.execute(
                module_bank_credentials.insert().values(
                    {
                        "access_token": token_json.get("accessToken"),
                        "integration_cashboxes": user_integration.get("id"),
                    }
                )
            )
        else:
            await database.execute(
                module_bank_credentials.update()
                .where(
                    module_bank_credentials.c.integration_cashboxes
                    == user_integration.get("id")
                )
                .values({"access_token": token_json.get("accessToken")})
            )
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))

    return RedirectResponse(
        f'https://app.tablecrm.com/integrations?token={user_integration.get("token")}'
    )


@router.get("/module_bank/get_oauth_link/")
async def get_token_for_scope(token: str, id_integration: int):
    """Получение токена для работы с разрешениями"""

    user = await get_user_by_token(token)
    user_integration = await integration_info(user.get("cashbox_id"), id_integration)

    link = (
        f'{user_integration.get("url")}authorize?'
        f'clientId={user_integration.get("client_app_id")}&'
        f'redirectUri={user_integration.get("redirect_uri")}&'
        f'scope={user_integration.get("scopes")}&'
        f'state={user.get("cashbox_id")}'
    )

    return {"link": link}


@router.get("/module_bank/integration_on")
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

        await manager.send_message(
            user.token,
            {
                "action": "on",
                "target": "IntegrationModuleBank",
                "integration_status": True,
                "integration_isAuth": True,
            },
        )
        return {"result": "ok"}
    except:
        raise HTTPException(
            status_code=422,
            detail="ошибка установки связи аккаунта пользователя и интеграции",
        )


@router.get("/module_bank/integration_off")
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
                "target": "IntegrationModuleBank",
                "integration_status": False,
            },
        )

        return {"isAuth": False}
    except:
        raise HTTPException(
            status_code=422,
            detail="ошибка удаления связи аккаунта пользователя и интеграции",
        )


@router.get("/module_bank/check")
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
        "target": "IntegrationModuleBank",
        "integration_status": check.get("status"),
    }

    isAuth = await database.fetch_one(
        module_bank_credentials.select().where(
            module_bank_credentials.c.integration_cashboxes == check.get("id")
        )
    )

    if isAuth:
        message.update({"integration_isAuth": True})
    else:
        message.update({"integration_isAuth": False})
    await manager.send_message(user.token, message)
    return {"isAuth": message.get("integration_isAuth")}


@router.get("/module_bank/accounts/")
async def accounts(token: str, id_integration: int):
    """Получение списка счетов аккаунта банка"""

    user = await get_user_by_token(token)

    query = (
        select(
            module_bank_accounts.c.id,
            pboxes.c.name,
            module_bank_accounts.c.currency,
            module_bank_accounts.c.is_active,
        )
        .where(
            and_(
                pboxes.c.cashbox == user.get("cashbox_id"),
                pboxes.c.deleted_at.is_(None),
            )
        )
        .select_from(pboxes)
        .join(module_bank_accounts, module_bank_accounts.c.payboxes_id == pboxes.c.id)
        .order_by(pboxes.c.name)
    )
    accounts = await database.fetch_all(query)
    return {"result": accounts}
