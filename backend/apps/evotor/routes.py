import aiohttp
from api.docs_sales.api.routers import create as createDocSales
from api.docs_sales.schemas import (
    Create,
    CreateMass as CreateMassDocSales,
)
from api.loyality_cards.routers import get_cards
from api.loyality_cards.schemas import LoyalityCardFilters
from api.loyality_settings.routers import get_loyality_settings
from api.nomenclature.routers import new_nomenclature
from api.nomenclature.schemas import NomenclatureCreateMass
from database.db import (
    database,
    evotor_credentials,
    integrations,
    integrations_to_cashbox,
    nomenclature,
    prices,
    users_cboxes_relation,
    warehouses,
)
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from functions.helpers import get_user_by_token
from sqlalchemy import and_, select
from ws_manager import manager

from .schemas import EvotorInstallEvent, EvotorUserToken, ListEvotorNomenclature

security = HTTPBearer()


async def get_token_evotor(cashbox_id: int, integration_id: int):
    query = (
        select(integrations_to_cashbox.c.id, evotor_credentials.c.evotor_token)
        .where(
            and_(
                integrations_to_cashbox.c.installed_by == cashbox_id,
                integrations_to_cashbox.c.integration_id == integration_id,
            )
        )
        .select_from(integrations_to_cashbox)
        .join(
            evotor_credentials,
            evotor_credentials.c.integration_cashboxes == integrations_to_cashbox.c.id,
        )
    )
    result = await database.fetch_one(query)
    if result:
        return result.get("evotor_token")
    else:
        raise HTTPException(status_code=432, detail="у пользователя нет токена Эвотор")


async def has_access(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    print(token, credentials)
    try:
        token_db = await database.fetch_one(
            integrations.select().where(integrations.c.id == 2)
        )
        if token_db:
            if token != token_db.get("client_secret"):
                raise AssertionError("not found integration")
        else:
            raise AssertionError("not found integration")
    except AssertionError as e:
        raise HTTPException(status_code=401, detail=str(e))


async def has_user(req: Request):
    try:
        await database.connect()
        evotor_user_id = req.headers.get("x-evotor-user-id")
        user_cashbox = await database.fetch_one(
            select(evotor_credentials.c.userId, users_cboxes_relation.c.token)
            .select_from(evotor_credentials)
            .join(
                integrations_to_cashbox,
                evotor_credentials.c.integration_cashboxes
                == integrations_to_cashbox.c.id,
            )
            .join(
                users_cboxes_relation,
                integrations_to_cashbox.c.installed_by == users_cboxes_relation.c.id,
            )
            .where(evotor_credentials.c.userId == evotor_user_id)
        )
        if user_cashbox:
            return user_cashbox.get("token")
        else:
            raise Exception("not found")
    except Exception as e:
        raise HTTPException(
            status_code=432,
            detail=f"ошибка аутентификации пользователя Эвотор (неверный userId) {e}",
        )


async def has_store(req: Request, token: str = Depends(has_user)):
    try:
        evotor_store_id = req.headers.get("x-evotor-store-uuid")
        if evotor_store_id:
            warehouse_db = await database.fetch_one(
                warehouses.select().where(warehouses.c.external_id == evotor_store_id)
            )
            if warehouse_db:
                return warehouse_db.get("id")
            else:
                user = await get_user_by_token(token)
                warehouse_id = await database.execute(
                    warehouses.insert().values(
                        {
                            "owner": user.get("id"),
                            "external_id": evotor_store_id,
                            "status": True,
                            "name": evotor_store_id,
                        }
                    )
                )
                return warehouse_id
    except Exception as e:
        raise HTTPException(status_code=432, detail=f"ошибка: {str(e)}")


router_auth = APIRouter(tags=["Evotor hook"], dependencies=[Depends(has_access)])
router = APIRouter(tags=["Evotor hook"])


@router_auth.post("/evotor/nomenclature")
async def events(data: ListEvotorNomenclature, req: Request):
    print(data, req.headers)


@router_auth.post("/evotor/events")
async def events(data: EvotorInstallEvent, req: Request):
    print(data, req.headers)


@router.put("/evotor/events")
async def events(req: Request):
    print(await req.json(), req.headers)


@router_auth.post("/evotor/user/token")
async def user_token(data: EvotorUserToken, req: Request):
    credential_check = await database.fetch_one(
        evotor_credentials.select().where(
            evotor_credentials.c.evotor_token == data.evotor_token
        )
    )
    if not credential_check:
        await database.execute(
            evotor_credentials.insert().values(
                {
                    "evotor_token": data.evotor_token,
                    "userId": data.userId,
                    "status": False,
                }
            )
        )
    else:
        await database.execute(
            evotor_credentials.update()
            .where(evotor_credentials.c.id == credential_check.get("id"))
            .values({"evotor_token": data.evotor_token})
        )


@router_auth.get("/evotor/loyality_cards/", dependencies=[Depends(has_store)])
async def loyality_cards(
    limit: int = 100,
    offset: int = 0,
    filters_q: LoyalityCardFilters = Depends(),
    token: str = Depends(has_user),
):

    return await get_cards(token=token, limit=limit, offset=offset, filters_q=filters_q)


@router_auth.get("/evotor/loyality_settings/")
async def loyality_cards(token: str = Depends(has_user)):

    return await get_loyality_settings(token=token)


@router_auth.post("/evotor/docs_sales/")
async def create_doc_sales(
    req: Request,
    docs_sales_data: CreateMassDocSales,
    generate_out: bool = True,
    token: str = Depends(has_user),
    warehouse_id: int = Depends(has_store),
):
    try:
        docs_data = []
        for item in docs_sales_data.__getattribute__("__root__"):
            item = dict(item)
            total_sum = 0.0
            doc_goods_data = []

            for good in item.get("goods"):
                if good.nomenclature:
                    good_db = await database.fetch_one(
                        nomenclature.select().where(
                            nomenclature.c.external_id == good.nomenclature
                        )
                    )
                    if good_db:
                        good.nomenclature = good_db.get("id")
                        doc_goods_data.append(good)
                    else:
                        user = await get_user_by_token(token)
                        token_evotor = await get_token_evotor(
                            cashbox_id=user.get("id"), integration_id=2
                        )
                        async with aiohttp.ClientSession(trust_env=True) as session:
                            async with session.get(
                                f"https://api.evotor.ru/stores/"
                                f'{req.headers.get("x-evotor-store-uuid")}'
                                f"/products/"
                                f"{good.nomenclature}",
                                headers={
                                    "Authorization": f"{token_evotor}",
                                    "Accept": "application/vnd.evotor.v2+json",
                                    "Content-Type": "application/vnd.evotor.v2+json",
                                },
                            ) as resp:
                                product = await resp.json()
                            await session.close()

                        good_id = await new_nomenclature(
                            token,
                            nomenclature_data=NomenclatureCreateMass(
                                __root__=[
                                    {
                                        "name": product.get("name"),
                                        "unit": 116,
                                        "external_id": good.nomenclature,
                                        "cashback_type": "lcard_cashback",
                                    }
                                ]
                            ),
                        )
                        good.nomenclature = good_id[0].get("id")

                        doc_goods_data.append(good)

                if good.price is None:
                    if isinstance(good.nomenclature, int):
                        price_query = prices.select().where(
                            prices.c.nomenclature == good.nomenclature,
                            prices.c.price_type == 1,
                            prices.c.is_deleted == False,
                        )
                        price_db = await database.fetch_one(price_query)
                        good.price = price_db.price if price_db else 0.0

                good.sum_discounted = good.price * good.quantity
                total_sum += good.sum_discounted

            item.update({"goods": doc_goods_data})
            item.update({"warehouse": warehouse_id})
            item.update({"sum": total_sum})
            docs_data.append(Create(**item))

        return await createDocSales(
            token=token,
            docs_sales_data=CreateMassDocSales(__root__=docs_data),
            generate_out=generate_out,
        )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=432, detail="ошибка создания документы продажи")


@router.get("/evotor/integration/on")
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
                "target": "IntegrationEvotor",
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


@database.transaction()
@router.get("/evotor/integration/off")
async def integration_off(token: str, id_integration: int):
    """Удаление связи аккаунта пользователя и интеграции"""

    user = await get_user_by_token(token)

    try:
        integration_cashbox = await database.fetch_one(
            integrations_to_cashbox.select().where(
                and_(
                    integrations_to_cashbox.c.integration_id == id_integration,
                    integrations_to_cashbox.c.installed_by == user.id,
                )
            )
        )

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

        await database.execute(
            evotor_credentials.update()
            .where(
                evotor_credentials.c.integration_cashboxes
                == integration_cashbox.get("id")
            )
            .values({"status": False})
        )

        await manager.send_message(
            user.token,
            {
                "action": "off",
                "target": "IntegrationEvotor",
                "integration_status": False,
            },
        )

        return {"isAuth": False}
    except Exception as e:
        print(e)
        raise HTTPException(
            status_code=422,
            detail="ошибка удаления связи аккаунта пользователя и интеграции",
        )


@database.transaction()
@router.get("/evotor/integration/install")
async def install(token: str, evotor_token: str, id_integration: int):
    user = await get_user_by_token(token)
    try:
        integration_cashbox = await database.fetch_one(
            integrations_to_cashbox.select().where(
                and_(
                    integrations_to_cashbox.c.integration_id == id_integration,
                    integrations_to_cashbox.c.installed_by == user.id,
                )
            )
        )

        status = await database.execute(
            evotor_credentials.update()
            .where(evotor_credentials.c.evotor_token == evotor_token)
            .values(
                {"integration_cashboxes": integration_cashbox.get("id"), "status": True}
            )
        )
        if status:
            print(status)
        else:
            raise HTTPException(
                status_code=422, detail="приложение не установлено в Эвотор"
            )
    except:
        raise HTTPException(
            status_code=422, detail="приложение не установлено в Эвотор"
        )


@router.get("/evotor/stores")
async def stores(token: str, id_integration: int):
    try:
        user = await get_user_by_token(token)
        token = await get_token_evotor(
            cashbox_id=user.get("id"), integration_id=id_integration
        )
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(
                "https://api.evotor.ru/stores",
                headers={
                    "Authorization": f"{token}",
                    "Accept": "application/vnd.evotor.v2+json",
                    "Content-Type": "application/vnd.evotor.v2+json",
                },
            ) as resp:
                stores = await resp.json()
            await session.close()
        return stores
    except Exception as e:
        raise HTTPException(status_code=432, detail=str(e))


@router.get("/evotor/integration/check")
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
        "target": "IntegrationEvotor",
        "integration_status": check.get("status"),
    }

    isAuth = await database.fetch_one(
        evotor_credentials.select().where(
            and_(
                evotor_credentials.c.integration_cashboxes == check.get("id"),
                evotor_credentials.c.status.is_not(False),
            )
        )
    )

    if isAuth:
        message.update({"integration_isAuth": True})
    else:
        message.update({"integration_isAuth": False})
    await manager.send_message(user.token, message)
    return {"isAuth": message.get("integration_isAuth")}
