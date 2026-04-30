from api.docs_warehouses.func_warehouse import (
    acquire_docs_warehouse_lock,
    set_data_doc_warehouse,
    update_docs_warehouse,
    update_goods_warehouse,
    validate_photo_for_writeoff,
)
from api.docs_warehouses.utils import upsert_warehouse_doc
from api.pagination.pagination import Page
from database.db import (
    OperationType,
    cashbox_settings,
    database,
    docs_warehouse,
    docs_warehouse_goods,
    nomenclature,
    organizations,
    units,
    warehouse_register_movement,
    warehouses,
)
from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi_pagination import add_pagination
from functions.helpers import (
    check_entity_exists,
    datetime_to_timestamp,
    get_user_by_token,
)
from functions.warehouse_events import (
    build_balance_recalc_events,
    publish_balance_recalc_batch,
)
from sqlalchemy import desc, func, select
from ws_manager import manager

from . import schemas

router = APIRouter(tags=["docs_warehouse"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()

Page = Page.with_custom_options(
    size=Query(10, ge=1, le=100),
)


async def _fetch_docs_warehouse_goods(
    doc_ids: list[int], *, unit_name_field: str
) -> dict[int, list[dict]]:
    if not doc_ids:
        return {}

    goods_rows = await database.fetch_all(
        docs_warehouse_goods.select().where(
            docs_warehouse_goods.c.docs_warehouse_id.in_(doc_ids)
        )
    )
    goods = [datetime_to_timestamp(row) for row in goods_rows]

    if not goods:
        return {}

    nomenclature_ids = {
        item["nomenclature"] for item in goods if item.get("nomenclature")
    }
    unit_ids = {item["unit"] for item in goods if item.get("unit")}

    nomenclature_rows = []
    if nomenclature_ids:
        nomenclature_rows = await database.fetch_all(
            nomenclature.select().where(nomenclature.c.id.in_(nomenclature_ids))
        )

    unit_rows = []
    if unit_ids:
        unit_rows = await database.fetch_all(
            units.select().where(units.c.id.in_(unit_ids))
        )

    nomenclature_by_id = {row["id"]: row["name"] for row in nomenclature_rows}
    unit_by_id = {row["id"]: row[unit_name_field] for row in unit_rows}

    goods_by_doc_id: dict[int, list[dict]] = {}
    for item in goods:
        item["nomenclature_name"] = nomenclature_by_id.get(item["nomenclature"], "")
        if item.get("unit"):
            item["unit_name"] = unit_by_id.get(item["unit"])
        goods_by_doc_id.setdefault(item["docs_warehouse_id"], []).append(item)

    return goods_by_doc_id


@router.get("/docs_warehouse/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)
    query = docs_warehouse.select().where(
        docs_warehouse.c.id == idx, docs_warehouse.c.is_deleted.is_not(True)
    )
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail="Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)

    goods_by_doc_id = await _fetch_docs_warehouse_goods([idx], unit_name_field="name")
    goods = [
        {
            "price_type": good["price_type"],
            "price": good["price"],
            "quantity": good["quantity"],
            "unit": good["unit"],
            "nomenclature": good["nomenclature"],
            "unit_name": good.get("unit_name"),
            "nomenclature_name": good.get("nomenclature_name", ""),
        }
        for good in goods_by_doc_id.get(idx, [])
    ]

    instance_db["goods"] = goods

    return instance_db


@router.get("/docs_warehouse/", response_model=schemas.GetDocsWarehouse)
async def get_list(
    token: str,
    warehouse_id: int = None,
    operation: str = "",
    show_goods: bool = False,
    limit: int = 10,
    offset: int = 0,
    datefrom: int = None,
    dateto: int = None,
    tags: str = None,
):
    """Получение списка документов"""
    filters_list = []
    user = await get_user_by_token(token)

    if datefrom and not dateto:
        filters_list.append(docs_warehouse.c.dated >= datefrom)
    if not datefrom and dateto:
        filters_list.append(docs_warehouse.c.dated <= dateto)
    if datefrom and dateto:
        filters_list.append(docs_warehouse.c.dated >= datefrom)
        filters_list.append(docs_warehouse.c.dated <= dateto)

    if tags:
        filters_list.append(docs_warehouse.c.tags.ilike(f"%{tags}%"))

    if operation:
        filters_list.append(docs_warehouse.c.operation == operation)

    if warehouse_id:
        filters_list.append(docs_warehouse.c.warehouse == warehouse_id)

    query = (
        docs_warehouse.select()
        .where(
            docs_warehouse.c.is_deleted.is_not(True),
            docs_warehouse.c.cashbox == user.cashbox_id,
        )
        .order_by(desc(docs_warehouse.c.id))
        .where(*filters_list)
        .limit(limit)
        .offset(offset)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if show_goods and items_db:
        goods_by_doc_id = await _fetch_docs_warehouse_goods(
            [item["id"] for item in items_db],
            unit_name_field="convent_national_view",
        )
        for item in items_db:
            item["goods"] = goods_by_doc_id.get(item["id"], [])

    query = (
        select(func.count(docs_warehouse.c.id))
        .where(
            docs_warehouse.c.is_deleted.is_not(True),
            docs_warehouse.c.cashbox == user.cashbox_id,
        )
        .where(*filters_list)
    )
    count = await database.fetch_one(query)

    return {"result": items_db, "count": count.count_1}


async def check_foreign_keys(instance_values, user, exceptions) -> bool:
    if instance_values.get("organization") is not None:
        if instance_values["organization"] not in organizations_cache:
            try:
                await check_entity_exists(
                    organizations, instance_values["organization"], user.id
                )
                organizations_cache.add(instance_values["organization"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("warehouse") is not None:
        if instance_values["warehouse"] not in warehouses_cache:
            try:
                await check_entity_exists(
                    warehouses, instance_values["warehouse"], user.id
                )
                warehouses_cache.add(instance_values["warehouse"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False
    return True


async def _resolve_missing_units(goods: list) -> list:
    if not goods:
        return goods

    missing_unit_nomenclature_ids = {
        item["nomenclature"]
        for item in goods
        if item.get("nomenclature") and not item.get("unit")
    }
    if not missing_unit_nomenclature_ids:
        return goods

    rows = await database.fetch_all(
        nomenclature.select().where(
            nomenclature.c.id.in_(missing_unit_nomenclature_ids)
        )
    )
    units_by_nomenclature = {row["id"]: row["unit"] for row in rows}

    for item in goods:
        if item.get("unit"):
            continue

        unit_id = units_by_nomenclature.get(item["nomenclature"])
        if not unit_id:
            unit_id = 116
        item["unit"] = unit_id

    return goods


async def _create_docs_warehouse_v2(
    token: str,
    docs_warehouse_data: schemas.CreateMass,
    *,
    holding: bool = False,
):
    response: list = []
    payload = docs_warehouse_data.dict()
    user = await get_user_by_token(token)

    for doc in payload["__root__"]:
        doc["goods"] = await _resolve_missing_units(doc.get("goods") or [])
        response.append(
            await upsert_warehouse_doc(
                token=token,
                doc=doc,
                cashbox_id=user.cashbox_id,
            )
        )

    query = docs_warehouse.select().where(docs_warehouse.c.id.in_(response))
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    if holding:
        await _update_docs_warehouse_v2(
            token,
            schemas.EditMass(
                __root__=[
                    {"id": doc["id"], "status": True} for doc in docs_warehouse_db
                ]
            ),
        )
        query = docs_warehouse.select().where(docs_warehouse.c.id.in_(response))
        docs_warehouse_db = await database.fetch_all(query)
        docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_warehouse",
            "result": docs_warehouse_db,
        },
    )
    return docs_warehouse_db


async def _update_docs_warehouse_v2(token: str, docs_warehouse_data: schemas.EditMass):
    response: list = []
    payload = docs_warehouse_data.dict(exclude_unset=True)

    # События баланса копим по всем документам и публикуем один раз
    # ПОСЛЕ выхода из всех транзакций — publish в RabbitMQ внутри открытой
    # tx держит сессию PG в idle in transaction и advisory lock до таймаута
    # (см. DOCS_WAREHOUSE_TX_HANG.md).
    pending_events: list = []

    for doc in payload["__root__"]:
        if doc.get("goods"):
            goods: list = doc["goods"]
            goods = await _resolve_missing_units(goods)
            del doc["goods"]
        else:
            goods = await database.fetch_all(
                docs_warehouse_goods.select().where(
                    docs_warehouse_goods.c.docs_warehouse_id == doc["id"]
                )
            )

        # Read-merge-write + rewrite goods/movements по одному doc_id
        # сериализуется через advisory_xact_lock, чтобы параллельные
        # patch'ы (в т.ч. fire-and-forget из docs_sales.patch) не удваивали строки.
        async with database.transaction():
            await acquire_docs_warehouse_lock(doc["id"])

            stored_item_data = await database.fetch_one(
                docs_warehouse.select().where(docs_warehouse.c.id == doc["id"])
            )
            if stored_item_data is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Документ склада {doc['id']} не найден",
                )
            stored_item_model = schemas.Edit(**stored_item_data)
            updated_item = stored_item_model.copy(update=doc)
            doc = jsonable_encoder(updated_item)
            del doc["goods"]

            entity = await set_data_doc_warehouse(entity_values=doc, token=token)
            if entity["operation"] == "write_off":
                if doc.get("status") is True:
                    cashbox_settings_data = await database.fetch_one(
                        cashbox_settings.select().where(
                            cashbox_settings.c.cashbox_id == entity["cashbox"]
                        )
                    )
                    require_photo = (
                        cashbox_settings_data
                        and cashbox_settings_data["require_photo_for_writeoff"]
                    )

                    if require_photo:
                        try:
                            await validate_photo_for_writeoff(doc["id"])
                        except HTTPException:
                            continue

            doc_id = await update_docs_warehouse(entity=entity)
            entity.update({"goods": goods})
            if entity["operation"] == "incoming":
                events = await update_goods_warehouse(
                    entity=entity, doc_id=doc_id, type_operation=OperationType.plus
                )
                if events:
                    pending_events.extend(events)
                response.append(doc_id)
            if entity["operation"] == "outgoing":
                events = await update_goods_warehouse(
                    entity=entity, doc_id=doc_id, type_operation=OperationType.minus
                )
                if events:
                    pending_events.extend(events)
                response.append(doc_id)
            if entity["operation"] == "transfer":
                events_minus = await update_goods_warehouse(
                    entity=entity, doc_id=doc_id, type_operation=OperationType.minus
                )
                if events_minus:
                    pending_events.extend(events_minus)
                entity.update({"warehouse": entity["to_warehouse"]})
                events_plus = await update_goods_warehouse(
                    entity=entity, doc_id=doc_id, type_operation=OperationType.plus
                )
                if events_plus:
                    pending_events.extend(events_plus)
                response.append(doc_id)
            if entity["operation"] == "write_off":
                events = await update_goods_warehouse(
                    entity=entity, doc_id=doc_id, type_operation=OperationType.minus
                )
                if events:
                    pending_events.extend(events)
                response.append(doc_id)

    if pending_events:
        await publish_balance_recalc_batch(pending_events)

    query = docs_warehouse.select().where(docs_warehouse.c.id.in_(response))
    docs_warehouse_db = await database.fetch_all(query)
    docs_warehouse_db = [*map(datetime_to_timestamp, docs_warehouse_db)]

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "docs_warehouse",
            "result": docs_warehouse_db,
        },
    )

    return docs_warehouse_db


@router.post("/docs_warehouse/", response_model=schemas.ListView)
async def create_legacy(token: str, docs_warehouse_data: schemas.CreateMass):
    """Создание документов через новую логику остатков"""
    return await _create_docs_warehouse_v2(token, docs_warehouse_data)


@router.patch("/docs_warehouse/", response_model=schemas.ListView)
async def update_legacy(token: str, docs_warehouse_data: schemas.EditMass):
    """Редактирование документов через новую логику остатков"""
    return await _update_docs_warehouse_v2(token, docs_warehouse_data)


@router.delete("/docs_warehouse/")
async def delete(token: str, ids: list[int]):
    """Удаление документов"""
    await get_user_by_token(token)

    query = docs_warehouse.select().where(
        docs_warehouse.c.id.in_(ids), docs_warehouse.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        recalc_events = []
        query = (
            docs_warehouse.update()
            .where(
                docs_warehouse.c.id.in_(ids), docs_warehouse.c.is_deleted.is_not(True)
            )
            .values({"is_deleted": True})
        )
        await database.execute(query)

        """ Изменение остатка на складе - удаление движения в регистре """
        try:
            for item in items_db:
                query = warehouse_register_movement.select().where(
                    warehouse_register_movement.c.document_warehouse_id == item["id"]
                )
                result = await database.fetch_all(query)
                item.update({"deleted": result})
                recalc_events.extend(build_balance_recalc_events(result))
                query = warehouse_register_movement.delete().where(
                    warehouse_register_movement.c.document_warehouse_id == item["id"]
                )
                await database.execute(query)
        except Exception as error:
            raise HTTPException(status_code=433, detail=str(error))

        if recalc_events:
            await publish_balance_recalc_batch(recalc_events)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_warehouse",
                "result": items_db,
            },
        )

    return items_db


@router.post(
    "/alt_docs_warehouse/",
    tags=["Alternative docs_warehouse"],
    response_model=schemas.ListView,
)
async def create(
    token: str, docs_warehouse_data: schemas.CreateMass, holding: bool = False
):
    """
    Создание документов движения товарных остатков
    operation:
        incoming Приходных (Увеличивает количество товара на складе)
        outgoing Расходных (Уменьшает количество товара на складе)
        transfer Переводных документов (Уменьшает на одном складе увеличивает на другом)
    """
    return await _create_docs_warehouse_v2(token, docs_warehouse_data, holding=holding)


@router.delete("/docs_warehouse/{idx}")
async def delete_docs_warehouse_route(token: str, idx: int):
    """Удаление документа"""
    await get_user_by_token(token)

    query = docs_warehouse.select().where(
        docs_warehouse.c.id == idx, docs_warehouse.c.is_deleted.is_not(True)
    )
    item_db = await database.fetch_one(query)
    item_db = datetime_to_timestamp(item_db)

    if item_db:
        recalc_events = []
        query = (
            docs_warehouse.update()
            .where(docs_warehouse.c.id == idx, docs_warehouse.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        """ Изменение остатка на складе - удаление движения в регистре """
        try:
            query = warehouse_register_movement.select().where(
                warehouse_register_movement.c.document_warehouse_id == item_db["id"]
            )
            result = await database.fetch_all(query)
            item_db.update({"deleted": result})
            recalc_events.extend(build_balance_recalc_events(result))
            query = warehouse_register_movement.delete().where(
                warehouse_register_movement.c.document_warehouse_id == item_db["id"]
            )
            await database.execute(query)
        except Exception as error:
            raise HTTPException(status_code=433, detail=str(error))

        if recalc_events:
            await publish_balance_recalc_batch(recalc_events)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_warehouse",
                "result": item_db,
            },
        )

    return item_db


@router.patch(
    "/alt_docs_warehouse/",
    tags=["Alternative docs_warehouse"],
    response_model=schemas.ListView,
)
async def update(token: str, docs_warehouse_data: schemas.EditMass):
    """
    Обновление
    """
    return await _update_docs_warehouse_v2(token, docs_warehouse_data)


add_pagination(router)
