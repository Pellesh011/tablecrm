import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

import httpx
from apps.cdek import schemas
from apps.cdek.client import CdekClient
from apps.cdek.models import (
    cdek_cashbox_settings,
    cdek_order_status_history,
    cdek_orders,
)
from apps.cdek.utils import (
    get_cdek_credentials,
    get_or_create_cdek_integration,
    integration_info,
    save_cdek_credentials,
)
from database.db import (
    contragents,
    database,
    docs_sales,
    docs_sales_delivery_info,
    docs_sales_goods,
    docs_sales_settings,
    integrations_to_cashbox,
    users_cboxes_relation,
)
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from functions.helpers import get_user_by_token
from sqlalchemy import desc, func, select
from ws_manager import manager

router = APIRouter(tags=["CDEK"])
logger = logging.getLogger(__name__)

LK_API_BASE = "https://preback.lk.cdek.ru"


@router.post("/cdek/lk/login")
async def cdek_lk_login(token: str, req: schemas.LkLoginRequest):
    """Вход в ЛК СДЭК, сохранение токена в БД и получение профиля."""
    user = await get_user_by_token(token)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{LK_API_BASE}/auth/login",
                json=req.dict(),
                headers={
                    "Content-Type": "application/json",
                    "X-Interface-Code": "selfcare",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            lk_token = data["token"]
            profile_resp = await client.post(
                f"{LK_API_BASE}/user/profile",
                headers={
                    "Authorization": f"Bearer {lk_token}",
                    "X-Interface-Code": "selfcare",
                },
            )
            profile_resp.raise_for_status()
            profile_data = profile_resp.json()
            settings = await database.fetch_one(
                cdek_cashbox_settings.select().where(
                    cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"]
                )
            )
            if settings:
                await database.execute(
                    cdek_cashbox_settings.update()
                    .where(cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"])
                    .values(lk_token=lk_token, updated_at=func.now())
                )
            else:
                await database.execute(
                    cdek_cashbox_settings.insert().values(
                        cashbox_id=user["cashbox_id"], lk_token=lk_token
                    )
                )

            return {"token": lk_token, "profile": profile_data.get("user")}
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code, detail=e.response.text
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/cdek/lk/integration-keys")
async def cdek_lk_integration_keys(token: str):
    """Получение ключей интеграции (account/secure) из ЛК."""
    user = await get_user_by_token(token)
    settings = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"]
        )
    )
    if not settings or not settings["lk_token"]:
        raise HTTPException(401, "No active ЛК session")

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                f"{LK_API_BASE}/integration/list",
                headers={
                    "Authorization": f"Bearer {settings['lk_token']}",
                    "X-Interface-Code": "selfcare",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            keys = data.get("keys", [])
            if not keys:
                raise HTTPException(404, "No integration keys found for this account")
            account = keys[0]["account"]
            secure = keys[0]["secure"]
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                await database.execute(
                    cdek_cashbox_settings.update()
                    .where(cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"])
                    .values(lk_token=None)
                )
            raise HTTPException(
                status_code=e.response.status_code, detail=e.response.text
            )
        except Exception as e:
            raise HTTPException(500, detail=str(e))

    integration_id = await get_or_create_cdek_integration()
    integ = await integration_info(user["cashbox_id"], integration_id)
    if not integ:
        new_integ_id = await database.execute(
            integrations_to_cashbox.insert().values(
                integration_id=integration_id,
                installed_by=user["id"],
                deactivated_by=user["id"],
                status=True,
            )
        )
        integ = {"id": new_integ_id, "installed_by": user["id"], "status": True}

    await save_cdek_credentials(integ["id"], account, secure)
    return {"status": "ok", "account": account}


@router.get("/cdek/lk/session")
async def cdek_lk_session(token: str):
    """Проверка наличия активной сессии ЛК и возврат профиля."""
    user = await get_user_by_token(token)
    settings = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"]
        )
    )
    if not settings or not settings["lk_token"]:
        return {"has_session": False}
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{LK_API_BASE}/user/profile",
                headers={
                    "Authorization": f"Bearer {settings['lk_token']}",
                    "X-Interface-Code": "selfcare",
                },
            )
            if resp.status_code == 200:
                profile = resp.json()
                return {"has_session": True, "profile": profile.get("user")}
            else:
                await database.execute(
                    cdek_cashbox_settings.update()
                    .where(cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"])
                    .values(lk_token=None)
                )
                return {"has_session": False}
        except Exception:
            return {"has_session": False}


@router.post("/cdek/lk/profile")
async def cdek_lk_profile(req: schemas.LkTokenRequest):
    """Прокси для получения профиля пользователя ЛК"""
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{LK_API_BASE}/user/profile",
                headers={
                    "Authorization": f"Bearer {req.token}",
                    "X-Interface-Code": "selfcare",
                },
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code, detail=e.response.text
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/cdek/lk/logout")
async def cdek_lk_logout(token: str):
    """Удаление сохранённого токена ЛК."""
    user = await get_user_by_token(token)
    await database.execute(
        cdek_cashbox_settings.update()
        .where(cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"])
        .values(lk_token=None)
    )
    return {"status": "ok"}


@router.post("/cdek/address-suggestions")
async def cdek_address_suggestions(token: str, req: schemas.AddressSuggestionRequest):
    """
    Прокси для получения подсказок адреса от ЛК СДЭК.
    Использует сохранённый токен ЛК для авторизации.
    """
    user = await get_user_by_token(token)
    settings = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"]
        )
    )
    if not settings or not settings["lk_token"]:
        raise HTTPException(
            status_code=401,
            detail="No active ЛК session. Please log in to your CDEK account.",
        )

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{LK_API_BASE}/address/adaptive-suggestions",
                json=req.dict(),
                headers={
                    "Authorization": f"Bearer {settings['lk_token']}",
                    "Content-Type": "application/json",
                    "X-Interface-Code": "selfcare",
                },
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                await database.execute(
                    cdek_cashbox_settings.update()
                    .where(cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"])
                    .values(lk_token=None)
                )
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"CDEK API error: {e.response.text}",
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/cdek/check")
async def cdek_check(token: str):
    """Проверка, установлена ли интеграция у клиента и есть ли учётные данные."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(204, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    is_auth = creds is not None and integ["status"] is True

    return {"isAuth": is_auth}


@router.get("/cdek/integration_on")
async def cdek_integration_on(token: str):
    """Активация интеграции."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    await database.execute(
        integrations_to_cashbox.update()
        .where(integrations_to_cashbox.c.id == integ["id"])
        .values(status=True)
    )

    await manager.send_message(
        user.token,
        {
            "action": "on",
            "target": "IntegrationCdek",
            "integration_status": True,
        },
    )
    return {"status": "ok"}


@router.get("/cdek/integration_off")
async def cdek_integration_off(token: str):
    """Деактивация интеграции."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    await database.execute(
        integrations_to_cashbox.update()
        .where(integrations_to_cashbox.c.id == integ["id"])
        .values(status=False)
    )

    await manager.send_message(
        user.token,
        {
            "action": "off",
            "target": "IntegrationCdek",
            "integration_status": False,
        },
    )
    return {"status": "ok"}


@router.get("/cdek/tariffs")
async def cdek_available_tariffs(token: str):
    """Получение списка доступных тарифов."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        result = await client.get_available_tariffs()
    except Exception as e:
        logger.exception("CDEK get tariffs error")
        raise HTTPException(500, f"Failed to get tariffs: {str(e)}")
    return {"tariff_codes": result}


@router.post("/cdek/calculate")
async def cdek_calculate(token: str, calc_req: schemas.CalculateRequest):
    """Расчёт стоимости доставки."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        services = None
        if calc_req.services:
            services = [s.dict(exclude_none=True) for s in calc_req.services]

        result = await client.calculate_tariff(
            from_location=calc_req.from_location.dict(exclude_none=True),
            to_location=calc_req.to_location.dict(exclude_none=True),
            packages=[p.dict(exclude_none=True) for p in calc_req.packages],
            tariff_code=calc_req.tariff_code,
            services=services,
            date=calc_req.date.isoformat() if calc_req.date else None,
            type=calc_req.type,
            currency=calc_req.currency,
            lang=calc_req.lang,
            additional_order_types=calc_req.additional_order_types,
        )
    except Exception as e:
        logger.exception("CDEK calculate error")
        raise HTTPException(500, f"Calculation failed: {str(e)}")

    return result


@router.get("/cdek/delivery-points")
async def cdek_delivery_points(
    token: str,
    city_code: Optional[int] = None,
    postal_code: Optional[str] = None,
    type: Optional[str] = None,
    have_cashless: Optional[bool] = None,
    allowed_cod: Optional[bool] = None,
):
    """Получение списка ПВЗ."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    filters = {
        "city_code": city_code,
        "postal_code": postal_code,
        "type": type,
        "have_cashless": have_cashless,
        "allowed_cod": allowed_cod,
    }
    try:
        points = await client.get_delivery_points(**filters)
    except Exception as e:
        logger.exception("CDEK get delivery points error")
        raise HTTPException(500, f"Failed to get delivery points: {str(e)}")

    return points


@router.get("/cdek/suggest-cities")
async def cdek_suggest_cities(token: str, name: str, country_code: str = "RU"):
    """
    Получение подсказок по населённым пунктам СДЭК.
    """
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        result = await client.suggest_cities(name, country_code)
    except Exception as e:
        logger.exception("CDEK suggest cities error")
        raise HTTPException(500, f"Failed to get city suggestions: {str(e)}")

    return result


@router.post("/cdek/order")
async def cdek_create_order(token: str, order_req: schemas.OrderRequest):
    """Создание заказа в СДЭК."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    order_data = order_req.dict(exclude_none=True)

    if not order_data.get("number"):
        order_data["number"] = (
            f"ORDER-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{user['cashbox_id']}"
        )

    try:
        result = await client.create_order(order_data)
    except Exception as e:
        logger.exception("CDEK create order error")
        raise HTTPException(500, f"Order creation failed: {str(e)}")

    requests_info = result.get("requests", [])
    if requests_info and requests_info[0].get("state") == "INVALID":
        logger.error(f"Order creation failed: {requests_info[0].get('errors')}")
        raise HTTPException(status_code=400, detail=result)

    entity = result.get("entity", {})
    order_uuid = entity.get("uuid")
    if order_uuid:
        await database.execute(
            cdek_orders.insert().values(
                order_uuid=order_uuid,
                cdek_number=entity.get("cdek_number"),
                number=order_req.number,
                status="CREATED",
                status_date=datetime.utcnow(),
                tariff_code=order_req.tariff_code,
                recipient_name=order_req.recipient.name,
                recipient_phone=(
                    order_req.recipient.phones[0]["number"]
                    if order_req.recipient.phones
                    else None
                ),
                delivery_point=order_req.delivery_point,
                cashbox_id=user["cashbox_id"],
                doc_sales_id=order_req.doc_sales_id,
                raw_data=str(result),
            )
        )
    return result


@router.get("/cdek/order/{order_uuid}")
async def cdek_order_info(token: str, order_uuid: str):
    """Получение информации о заказе по UUID СДЭК."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        result = await client.get_order_by_uuid(order_uuid)
    except Exception as e:
        logger.exception("CDEK get order info error")
        raise HTTPException(500, f"Failed to get order info: {str(e)}")

    entity = result.get("entity", {})
    if entity:
        await database.execute(
            cdek_orders.update()
            .where(cdek_orders.c.order_uuid == order_uuid)
            .values(
                cdek_number=entity.get("cdek_number"),
                status=entity.get("status"),
                status_date=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )

    return result


@router.get("/cdek/orders")
async def cdek_get_orders(
    token: str,
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
):
    """
    Получение списка заказов СДЭК с полными данными о связанных продажах.
    """
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    query = (
        select(
            cdek_orders,
            docs_sales.c.number.label("doc_number"),
            docs_sales.c.sum.label("doc_sum"),
            docs_sales.c.dated.label("doc_dated"),
            docs_sales.c.comment.label("doc_comment"),
            docs_sales.c.contragent.label("doc_contragent_id"),
            docs_sales.c.organization.label("doc_organization_id"),
            docs_sales.c.warehouse.label("doc_warehouse_id"),
            docs_sales.c.settings.label("doc_settings_id"),
            contragents.c.name.label("contragent_name"),
            contragents.c.phone.label("contragent_phone"),
            contragents.c.inn.label("contragent_inn"),
            contragents.c.email.label("contragent_email"),
        )
        .select_from(
            cdek_orders.join(
                docs_sales, cdek_orders.c.doc_sales_id == docs_sales.c.id
            ).join(contragents, docs_sales.c.contragent == contragents.c.id)
        )
        .where(cdek_orders.c.cashbox_id == user["cashbox_id"])
    )
    if status:
        query = query.where(cdek_orders.c.status == status)
    query = query.limit(limit).offset(offset)

    rows = await database.fetch_all(query)
    items = [dict(row) for row in rows]

    await enrich_cdek_orders_with_docs_sales(items, user["cashbox_id"])

    for item in items:
        history = await database.fetch_all(
            select(cdek_order_status_history)
            .where(cdek_order_status_history.c.order_uuid == item["order_uuid"])
            .order_by(desc(cdek_order_status_history.c.date_time))
            .limit(5)
        )
        item["status_history"] = [dict(h) for h in history]

    count_query = (
        select(func.count())
        .select_from(cdek_orders)
        .where(cdek_orders.c.cashbox_id == user["cashbox_id"])
    )
    if status:
        count_query = count_query.where(cdek_orders.c.status == status)
    total = await database.execute(count_query)

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


async def enrich_cdek_orders_with_docs_sales(items: List[Dict], cashbox_id: int):
    """
    Обогащает список заказов СДЭК полными данными о связанных документах продажи.
    Добавляет в каждый элемент ключ 'doc_sales' с полным объектом продажи.
    """
    doc_ids = [item["doc_sales_id"] for item in items if item.get("doc_sales_id")]

    if not doc_ids:
        return

    goods_query = docs_sales_goods.select().where(
        docs_sales_goods.c.docs_sales_id.in_(doc_ids)
    )
    goods_rows = await database.fetch_all(goods_query)
    goods_by_doc = defaultdict(list)
    for g in goods_rows:
        goods_by_doc[g["docs_sales_id"]].append(dict(g))

    delivery_query = docs_sales_delivery_info.select().where(
        docs_sales_delivery_info.c.docs_sales_id.in_(doc_ids)
    )
    delivery_rows = await database.fetch_all(delivery_query)
    delivery_by_doc = {d["docs_sales_id"]: dict(d) for d in delivery_rows}

    payments_query = """
        SELECT p.*, e.from_id as docs_sales_id
        FROM payments p
        JOIN entity_to_entity e ON e.to_id = p.id
        WHERE e.from_entity = 7 AND e.to_entity = 5
          AND e.from_id = ANY(:doc_ids)
          AND p.cashbox = :cashbox_id
    """
    payments_rows = await database.fetch_all(
        payments_query, {"doc_ids": doc_ids, "cashbox_id": cashbox_id}
    )
    payments_by_doc = defaultdict(list)
    for p in payments_rows:
        payments_by_doc[p["docs_sales_id"]].append(dict(p))

    loyality_query = """
        SELECT lt.*, e.from_id as docs_sales_id
        FROM loyality_transactions lt
        JOIN entity_to_entity e ON e.to_id = lt.id
        WHERE e.from_entity = 7 AND e.to_entity = 6
          AND e.from_id = ANY(:doc_ids)
          AND lt.cashbox = :cashbox_id
    """
    loyality_rows = await database.fetch_all(
        loyality_query, {"doc_ids": doc_ids, "cashbox_id": cashbox_id}
    )
    loyality_by_doc = defaultdict(list)
    for lt in loyality_rows:
        loyality_by_doc[lt["docs_sales_id"]].append(dict(lt))

    settings_ids = [
        item["doc_settings_id"] for item in items if item.get("doc_settings_id")
    ]
    if settings_ids:
        settings_query = docs_sales_settings.select().where(
            docs_sales_settings.c.id.in_(settings_ids)
        )
        settings_rows = await database.fetch_all(settings_query)
        settings_by_id = {s["id"]: dict(s) for s in settings_rows}
    else:
        settings_by_id = {}

    for item in items:
        doc_id = item.get("doc_sales_id")
        if not doc_id:
            continue

        doc_data = {
            "id": doc_id,
            "number": item.get("doc_number"),
            "sum": item.get("doc_sum"),
            "dated": item.get("doc_dated"),
            "comment": item.get("doc_comment"),
            "contragent_id": item.get("doc_contragent_id"),
            "organization_id": item.get("doc_organization_id"),
            "warehouse_id": item.get("doc_warehouse_id"),
            "contragent": (
                {
                    "name": item.get("contragent_name"),
                    "phone": item.get("contragent_phone"),
                    "inn": item.get("contragent_inn"),
                    "email": item.get("contragent_email"),
                }
                if item.get("contragent_name")
                else None
            ),
            "goods": goods_by_doc.get(doc_id, []),
            "delivery_info": delivery_by_doc.get(doc_id),
            "payments": payments_by_doc.get(doc_id, []),
            "loyality_transactions": loyality_by_doc.get(doc_id, []),
            "settings": settings_by_id.get(item.get("doc_settings_id")),
        }

        item["doc_sales"] = doc_data


@router.patch("/cdek/order/{order_uuid}/link-sales")
async def cdek_link_order_to_sales(
    token: str, order_uuid: str, req: schemas.LinkSalesRequest
):
    """
    Привязывает заказ СДЭК к документу продажи.
    """
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    order = await database.fetch_one(
        cdek_orders.select().where(
            cdek_orders.c.order_uuid == order_uuid,
            cdek_orders.c.cashbox_id == user["cashbox_id"],
        )
    )
    if not order:
        raise HTTPException(404, "Order not found")

    doc = await database.fetch_one(
        docs_sales.select().where(
            docs_sales.c.id == req.doc_sales_id,
            docs_sales.c.cashbox == user["cashbox_id"],
        )
    )
    if not doc:
        raise HTTPException(
            404, "Sales document not found or does not belong to your cashbox"
        )

    await database.execute(
        cdek_orders.update()
        .where(cdek_orders.c.order_uuid == order_uuid)
        .values(doc_sales_id=req.doc_sales_id, updated_at=datetime.utcnow())
    )

    return {"status": "ok", "order_uuid": order_uuid, "doc_sales_id": req.doc_sales_id}


@router.delete("/cdek/order/{order_uuid}")
async def cdek_delete_order(token: str, order_uuid: str):
    """Удаление заказа СДЭК."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    order = await database.fetch_one(
        cdek_orders.select().where(
            cdek_orders.c.order_uuid == order_uuid,
            cdek_orders.c.cashbox_id == user["cashbox_id"],
        )
    )
    if not order:
        raise HTTPException(404, "Order not found in local database")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        result = await client.delete_order(order_uuid)
    except Exception as e:
        error_str = str(e)
        if (
            "404" in error_str
            or "not found" in error_str.lower()
            or "v2_entity_not_found" in error_str
        ):
            await database.execute(
                cdek_order_status_history.delete().where(
                    cdek_order_status_history.c.order_uuid == order_uuid
                )
            )
            await database.execute(
                cdek_orders.delete().where(cdek_orders.c.order_uuid == order_uuid)
            )
            logger.info(f"Order {order_uuid} not found in CDEK, removed from local DB")
            return {
                "status": "ok",
                "message": "Order removed locally (not found in CDEK)",
            }
        else:
            logger.exception("CDEK delete order error")
            raise HTTPException(500, f"Order deletion failed: {str(e)}")

    await database.execute(
        cdek_order_status_history.delete().where(
            cdek_order_status_history.c.order_uuid == order_uuid
        )
    )
    await database.execute(
        cdek_orders.delete().where(cdek_orders.c.order_uuid == order_uuid)
    )
    return result


@router.get("/cdek/order/by-doc/{doc_sales_id}")
async def cdek_get_order_by_doc(token: str, doc_sales_id: int):
    """
    Получение заказа СДЭК, привязанного к документу продажи.
    """
    user = await get_user_by_token(token)

    doc = await database.fetch_one(
        docs_sales.select().where(
            docs_sales.c.id == doc_sales_id, docs_sales.c.cashbox == user["cashbox_id"]
        )
    )
    if not doc:
        raise HTTPException(404, "Sales document not found")

    order = await database.fetch_one(
        cdek_orders.select().where(
            cdek_orders.c.doc_sales_id == doc_sales_id,
            cdek_orders.c.cashbox_id == user["cashbox_id"],
        )
    )
    if not order:
        raise HTTPException(404, "No CDEK order linked to this document")

    return dict(order)


@router.get("/cdek/template")
async def cdek_get_template(token: str):
    """Получение сохранённого шаблона заказа."""
    user = await get_user_by_token(token)
    settings = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"]
        )
    )
    if settings and settings["template"]:
        return settings["template"]
    return {}


@router.post("/cdek/template")
async def cdek_save_template(token: str, template: dict):
    """Сохранение шаблона заказа."""
    user = await get_user_by_token(token)
    settings = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"]
        )
    )
    if settings:
        await database.execute(
            cdek_cashbox_settings.update()
            .where(cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"])
            .values(template=template, updated_at=func.now())
        )
    else:
        await database.execute(
            cdek_cashbox_settings.insert().values(
                cashbox_id=user["cashbox_id"], template=template
            )
        )
    return {"status": "ok"}


@router.post("/cdek/print/waybill")
async def cdek_print_waybill(token: str, req: schemas.WaybillRequest):
    """Формирование квитанции к заказу."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        result = await client.print_waybill(req.dict(exclude_none=True))
    except Exception as e:
        logger.exception("CDEK print waybill error")
        raise HTTPException(500, f"Print waybill failed: {str(e)}")

    return result


@router.get("/cdek/print/waybill/{uuid}")
async def cdek_get_waybill_info(token: str, uuid: str):
    """Получение информации о квитанции."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        result = await client.get_waybill_info(uuid)
    except Exception as e:
        logger.exception("CDEK get waybill info error")
        raise HTTPException(500, f"Get waybill info failed: {str(e)}")

    return result


@router.get("/cdek/print/waybill/{uuid}/download")
async def cdek_download_waybill(token: str, uuid: str):
    """Скачивание готовой квитанции в PDF."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        pdf_content = await client.download_waybill(uuid)
    except Exception as e:
        logger.exception("CDEK download waybill error")
        raise HTTPException(500, f"Download waybill failed: {str(e)}")

    return Response(
        content=pdf_content,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=waybill_{uuid}.pdf"},
    )


@router.post("/cdek/print/barcode")
async def cdek_print_barcode(token: str, req: schemas.BarcodeRequest):
    """Формирование ШК места к заказу."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        result = await client.print_barcode(req.dict(exclude_none=True))
    except Exception as e:
        logger.exception("CDEK print barcode error")
        raise HTTPException(500, f"Print barcode failed: {str(e)}")

    return result


@router.get("/cdek/print/barcode/{uuid}")
async def cdek_get_barcode_info(token: str, uuid: str):
    """Получение информации о ШК."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        result = await client.get_barcode_info(uuid)
    except Exception as e:
        logger.exception("CDEK get barcode info error")
        raise HTTPException(500, f"Get barcode info failed: {str(e)}")

    return result


@router.get("/cdek/print/barcode/{uuid}/download")
async def cdek_download_barcode(token: str, uuid: str):
    """Скачивание готового ШК в PDF."""
    user = await get_user_by_token(token)
    integ = await integration_info(
        user["cashbox_id"], await get_or_create_cdek_integration()
    )
    if not integ:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(integ["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    client = CdekClient(creds["account"], creds["secure_password"])
    try:
        pdf_content = await client.download_barcode(uuid)
    except Exception as e:
        logger.exception("CDEK download barcode error")
        raise HTTPException(500, f"Download barcode failed: {str(e)}")

    return Response(
        content=pdf_content,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=barcode_{uuid}.pdf"},
    )


FIXED_ACCOUNT = "ZJ1BdyNbUkBbWnOmDUEin0vY99VXiYVj"
FIXED_SECURE_PASSWORD = "iexsg0GiTvdT1r9XBDQ27cOFmsDQKBhX"
CASHBOX_ID = 229


@router.post("/cdek/order/kavkaz/calculate", include_in_schema=False)
async def cdek_calculate_client(req: schemas.ClientCalculateRequest):
    settings = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.template.isnot(None)
        )
    )
    if not settings or not settings["template"]:
        raise HTTPException(
            400, "No sender template configured. Please configure template first."
        )
    template = settings["template"]
    client = CdekClient(FIXED_ACCOUNT, FIXED_SECURE_PASSWORD)
    try:
        filtered_tariffs = await client._get_filtered_tariffs(
            items=req.items,
            delivery_type=req.delivery_type,
            address=req.address,
            delivery_point=req.delivery_point,
            recipient_city_code=req.recipient_city_code,
            template=template,
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception("Unexpected error in tariff filtering")
        raise HTTPException(500, str(e))

    return {"tariffs": filtered_tariffs}


@router.post("/cdek/order/kavkaz", include_in_schema=False)
async def cdek_client_order(req: schemas.ClientOrderRequest):
    settings = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.template.isnot(None)
        )
    )
    if not settings or not settings["template"]:
        raise HTTPException(
            400, "No sender template configured. Please configure template first."
        )
    template = settings["template"]
    client = CdekClient(FIXED_ACCOUNT, FIXED_SECURE_PASSWORD)
    try:
        filtered_tariffs = await client._get_filtered_tariffs(
            items=req.items,
            delivery_type=req.delivery_type,
            address=req.address,
            delivery_point=req.delivery_point,
            recipient_city_code=req.recipient_city_code,
            template=template,
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception("Unexpected error in tariff filtering")
        raise HTTPException(500, str(e))

    if not filtered_tariffs:
        raise HTTPException(
            400, "No suitable tariffs available for the given parameters"
        )

    if req.tariff_code:
        selected = next(
            (t for t in filtered_tariffs if t["tariff_code"] == req.tariff_code), None
        )
        if not selected:
            raise HTTPException(400, f"Tariff code {req.tariff_code} not available")
    else:
        selected = min(filtered_tariffs, key=lambda t: t["delivery_sum"])

    sender_city_data = template.get("sender_city_data")
    pickup_mode = template.get("pickup_mode", "door")

    total_weight = sum(item.weight * item.amount for item in req.items) or 1000
    packages = [
        {
            "number": "1",
            "weight": total_weight,
            "length": 30,
            "width": 20,
            "height": 20,
            "items": [
                {
                    "name": item.name,
                    "ware_key": item.ware_key,
                    "cost": item.cost,
                    "weight": item.weight,
                    "amount": item.amount,
                    "payment": {"value": item.cost * item.amount},
                }
                for item in req.items
            ],
        }
    ]

    order_data = {
        "type": 1,
        "tariff_code": selected["tariff_code"],
        "sender": {
            "name": template.get("sender_fio", ""),
            "company": template.get("sender_company", ""),
            "phones": [{"number": template.get("sender_phone", "")}],
            "tin": template.get("sender_inn", ""),
        },
        "recipient": {
            "name": req.recipient_name,
            "phones": [{"number": req.recipient_phone}],
        },
        "packages": packages,
    }
    if req.recipient_email:
        order_data["recipient"]["email"] = req.recipient_email

    if pickup_mode == "door":
        order_data["from_location"] = {
            "code": sender_city_data["code"],
            "city": sender_city_data.get("name"),
            "address": template.get("pickup_address")
            or template.get("sender_address", ""),
        }
    else:
        if not template.get("pickup_pvz_data") or not template["pickup_pvz_data"].get(
            "code"
        ):
            raise HTTPException(400, "Pickup PVZ data missing in template")
        order_data["shipment_point"] = template["pickup_pvz_data"]["code"]

    if req.delivery_type == "courier":
        order_data["to_location"] = {
            "code": req.recipient_city_code,
            "address": req.address,
        }
    else:
        order_data["delivery_point"] = req.delivery_point
    try:
        result = await client.create_order(order_data)
    except Exception as e:
        logger.exception("CDEK client order error")
        raise HTTPException(500, f"Order creation failed: {str(e)}")

    requests_info = result.get("requests", [])
    if requests_info and requests_info[0].get("state") == "INVALID":
        logger.error(f"Client order creation failed: {requests_info[0].get('errors')}")
        raise HTTPException(400, detail=result)

    entity = result.get("entity", {})
    order_uuid = entity.get("uuid")
    if order_uuid:
        await database.execute(
            cdek_orders.insert().values(
                order_uuid=order_uuid,
                cdek_number=entity.get("cdek_number"),
                number=order_data.get("number"),
                status="CREATED",
                status_date=datetime.utcnow(),
                tariff_code=selected["tariff_code"],
                recipient_name=req.recipient_name,
                recipient_phone=req.recipient_phone,
                delivery_point=req.delivery_point,
                cashbox_id=settings["cashbox_id"],
                doc_sales_id=req.doc_sales_id,
                raw_data=str(result),
            )
        )

    return result


@router.get("/cdek/delivery-points/kavkaz", include_in_schema=False)
async def cdek_delivery_points_client(
    city_code: Optional[int] = None,
    postal_code: Optional[str] = None,
    type: Optional[str] = None,
    have_cashless: Optional[bool] = None,
    allowed_cod: Optional[bool] = None,
):
    client = CdekClient(FIXED_ACCOUNT, FIXED_SECURE_PASSWORD)
    filters = {
        k: v
        for k, v in {
            "city_code": city_code,
            "postal_code": postal_code,
            "type": type,
            "have_cashless": have_cashless,
            "allowed_cod": allowed_cod,
        }.items()
        if v is not None
    }
    try:
        points = await client.get_delivery_points(**filters)
    except Exception as e:
        logger.exception("CDEK get delivery points client error")
        raise HTTPException(500, f"Failed to get delivery points: {str(e)}")
    return points


@router.get("/cdek/suggest-cities/kavkaz", include_in_schema=False)
async def cdek_suggest_cities_client(name: str, country_code: str = "RU"):
    client = CdekClient(FIXED_ACCOUNT, FIXED_SECURE_PASSWORD)
    try:
        result = await client.suggest_cities(name, country_code)
    except Exception as e:
        logger.exception("CDEK suggest cities client error")
        raise HTTPException(500, f"Failed to get city suggestions: {str(e)}")
    return result


@router.post("/cdek/webhooks/auto-register")
async def cdek_auto_register_webhook(token: str, request: Request):
    user = await get_user_by_token(token)
    integ = await get_or_create_cdek_integration()
    info = await integration_info(user["cashbox_id"], integ)
    if not info:
        raise HTTPException(404, "Integration not installed")

    creds = await get_cdek_credentials(info["id"])
    if not creds:
        raise HTTPException(400, "CDEK credentials not found")

    base_url = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
    if not base_url:
        scheme = request.headers.get("x-forwarded-proto", "https")
        host = request.headers.get("x-forwarded-host") or request.headers.get(
            "host", ""
        )
        base_url = f"{scheme}://{host}"

    webhook_url = f"{base_url}/api/v1/cdek/webhook"
    client = CdekClient(creds["account"], creds["secure_password"])

    # Попытка получить список существующих вебхуков
    try:
        existing_webhooks = await client._request("GET", "/webhooks")
        # Ответ может быть списком или содержать поле "items"
        if isinstance(existing_webhooks, list):
            webhook_list = existing_webhooks
        elif isinstance(existing_webhooks, dict):
            webhook_list = existing_webhooks.get("items", [])
        else:
            webhook_list = []

        for wh in webhook_list:
            if wh.get("type") == "ORDER_STATUS" and wh.get("url") == webhook_url:
                await _save_webhook_uuid(user["cashbox_id"], wh["uuid"])
                return {
                    "registered": True,
                    "webhook_uuid": wh["uuid"],
                    "already_existed": True,
                }
    except Exception as e:
        logger.warning(f"Failed to fetch existing webhooks: {e}")

    try:
        result = await client._request(
            "POST",
            "/webhooks",
            json={"type": "ORDER_STATUS", "url": webhook_url},
        )
    except Exception as e:
        error_text = str(e)
        if "already exists" in error_text and "UUID:" in error_text:
            import re

            match = re.search(r"UUID:\s*([0-9a-f-]+)", error_text, re.IGNORECASE)
            if match:
                webhook_uuid = match.group(1)
                await _save_webhook_uuid(user["cashbox_id"], webhook_uuid)
                return {
                    "registered": True,
                    "webhook_uuid": webhook_uuid,
                    "already_existed": True,
                }
        logger.exception("CDEK webhook registration failed")
        raise HTTPException(500, f"CDEK webhook registration failed: {e}")

    webhook_uuid = result.get("entity", {}).get("uuid") or result.get("requests", [{}])[
        0
    ].get("uuid")

    if webhook_uuid:
        await _save_webhook_uuid(user["cashbox_id"], webhook_uuid)

    return {"registered": True, "webhook_uuid": webhook_uuid, "already_existed": False}


@router.get("/cdek/webhooks/status")
async def cdek_webhook_status(token: str):
    user = await get_user_by_token(token)
    row = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.cashbox_id == user["cashbox_id"]
        )
    )
    if not row:
        return {"registered": False}

    template = row["template"] or {}
    if isinstance(template, str):
        try:
            template = json.loads(template)
        except Exception:
            template = {}

    webhook_uuid = template.get("webhook", {}).get("uuid")
    return {"registered": bool(webhook_uuid), "webhook_uuid": webhook_uuid}


async def _save_webhook_uuid(cashbox_id: int, webhook_uuid: str):
    row = await database.fetch_one(
        cdek_cashbox_settings.select().where(
            cdek_cashbox_settings.c.cashbox_id == cashbox_id
        )
    )

    template_data = {}
    if row and row["template"]:
        if isinstance(row["template"], str):
            try:
                template_data = json.loads(row["template"])
            except Exception:
                template_data = {}
        else:
            template_data = row["template"]

    template_data["webhook"] = {
        "uuid": webhook_uuid,
        "registered_at": datetime.utcnow().isoformat(),
    }

    if row:
        await database.execute(
            cdek_cashbox_settings.update()
            .where(cdek_cashbox_settings.c.cashbox_id == cashbox_id)
            .values(template=template_data)
        )
    else:
        await database.execute(
            cdek_cashbox_settings.insert().values(
                cashbox_id=cashbox_id, template=template_data
            )
        )


@router.post("/cdek/webhook")
async def cdek_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    logger.info(f"CDEK webhook: {data}")

    event_type = data.get("type")

    if event_type == "ORDER_STATUS":
        order_uuid = data.get("uuid")
        attributes = data.get("attributes", {})

        status_code = attributes.get("code")
        status_date = attributes.get("status_date_time")
        city = attributes.get("city_name")
        reason_code = attributes.get("status_reason_code")
        cdek_number = attributes.get("cdek_number")
        im_number = attributes.get("number")
        is_return = attributes.get("is_return", False)
        is_reverse = attributes.get("is_reverse", False)
        deleted = attributes.get("deleted", False)

        if not order_uuid or not status_code:
            logger.warning(f"CDEK webhook ORDER_STATUS missing uuid or code: {data}")
            return JSONResponse(status_code=200, content={"result": "ok"})

        if deleted:
            logger.info(
                f"CDEK webhook: status {status_code} deleted for order {order_uuid}, skipping"
            )
            return JSONResponse(status_code=200, content={"result": "ok"})

        await database.execute(
            cdek_orders.update()
            .where(cdek_orders.c.order_uuid == order_uuid)
            .values(
                status=status_code,
                cdek_number=cdek_number or cdek_orders.c.cdek_number,
                status_date=status_date,
                updated_at=datetime.utcnow(),
            )
        )

        await database.execute(
            cdek_order_status_history.insert().values(
                order_uuid=order_uuid,
                status_code=status_code,
                status_name=None,
                date_time=status_date or datetime.utcnow(),
                city=city,
                reason_code=reason_code,
                reason_description=None,
            )
        )

        order = await database.fetch_one(
            cdek_orders.select().where(cdek_orders.c.order_uuid == order_uuid)
        )
        if order:
            users = await database.fetch_all(
                users_cboxes_relation.select().where(
                    users_cboxes_relation.c.cashbox_id == order["cashbox_id"]
                )
            )
            ws_payload = {
                "target": "cdek_status",
                "action": "update",
                "order_uuid": order_uuid,
                "status": status_code,
                "cdek_number": cdek_number,
                "doc_sales_id": order["doc_sales_id"],
                "city": city,
                "is_return": is_return,
                "is_reverse": is_reverse,
                "date_time": str(status_date) if status_date else None,
            }
            for user in users:
                try:
                    await manager.send_message(user["token"], ws_payload)
                except Exception:
                    pass

    return JSONResponse(status_code=200, content={"result": "ok"})
