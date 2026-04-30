import uuid
from typing import Optional

from api.docs_purchases.rabbitmq.messages.CreatePurchaseAutoExpenseMessage import (
    CreatePurchaseAutoExpenseMessage,
)
from api.docs_warehouses.utils import create_warehouse_docs
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc
from database.db import (
    contracts,
    database,
    docs_purchases,
    docs_purchases_goods,
    docs_warehouse,
    nomenclature,
    organizations,
    price_types,
    units,
    users_cboxes_relation,
    warehouses,
)
from fastapi import APIRouter, HTTPException
from functions.helpers import (
    check_contragent_exists,
    check_entity_exists,
    check_period_blocked,
    check_unit_exists,
    datetime_to_timestamp,
    get_user_by_token,
)
from sqlalchemy import desc, func, select, text
from ws_manager import manager

from . import schemas

router = APIRouter(tags=["docs_purchases"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()
price_types_cache = set()
units_cache = set()
nomenclature_cache = set()


async def _get_purchase_product_goods(instance_id: int) -> list[dict]:
    goods_rows = await database.fetch_all(
        docs_purchases_goods.select().where(
            docs_purchases_goods.c.docs_purchases_id == instance_id
        )
    )
    if not goods_rows:
        return []

    nomenclature_ids = {int(item["nomenclature"]) for item in goods_rows}
    nomenclature_rows = await database.fetch_all(
        nomenclature.select().where(nomenclature.c.id.in_(nomenclature_ids))
    )
    product_nomenclature_ids = {
        row["id"] for row in nomenclature_rows if row["type"] == "product"
    }

    result = []
    for item in goods_rows:
        nomenclature_id = int(item["nomenclature"])
        if nomenclature_id not in product_nomenclature_ids:
            continue
        result.append(
            {
                "price_type": item.get("price_type") or 1,
                "price": item.get("price") or 0,
                "quantity": item["quantity"],
                "unit": item.get("unit"),
                "nomenclature": nomenclature_id,
            }
        )

    return result


async def _sync_purchase_incoming_doc(token: str, cashbox_id: int, instance_id: int):
    purchase_db = await database.fetch_one(
        docs_purchases.select().where(
            docs_purchases.c.id == instance_id,
            docs_purchases.c.cashbox == cashbox_id,
            docs_purchases.c.is_deleted.is_not(True),
        )
    )
    if not purchase_db or purchase_db.warehouse is None:
        return

    goods_res = await _get_purchase_product_goods(instance_id)
    existing_wh = await database.fetch_one(
        docs_warehouse.select()
        .where(
            docs_warehouse.c.cashbox == cashbox_id,
            docs_warehouse.c.is_deleted.is_not(True),
            docs_warehouse.c.operation == "incoming",
            docs_warehouse.c.docs_purchases == instance_id,
        )
        .order_by(desc(docs_warehouse.c.id))
    )

    if not goods_res and not existing_wh:
        return

    body = {
        "number": None,
        "dated": purchase_db.dated,
        "docs_purchases": instance_id,
        "docs_sales_id": None,
        "to_warehouse": None,
        "organization": purchase_db.organization,
        "status": bool(purchase_db.status),
        "contragent": purchase_db.contragent,
        "operation": "incoming",
        "comment": purchase_db.comment,
        "warehouse": purchase_db.warehouse,
        "goods": goods_res,
    }
    if existing_wh:
        body["id"] = existing_wh.id
        body["number"] = existing_wh.number

    await create_warehouse_docs(token, body, cashbox_id)


async def _find_existing_purchase_id(
    instance_values: dict, cashbox_id: int
) -> Optional[int]:
    number = instance_values.get("number")
    if not number:
        return None

    filters = [
        docs_purchases.c.cashbox == cashbox_id,
        docs_purchases.c.is_deleted.is_not(True),
        docs_purchases.c.number == str(number),
    ]

    row = await database.fetch_one(
        select(docs_purchases.c.id).where(*filters).order_by(desc(docs_purchases.c.id))
    )
    return int(row["id"]) if row else None


async def _replace_purchase_goods(
    *,
    instance_id: int,
    goods: list,
    user,
    exceptions: list[str],
) -> float:
    await database.execute(
        docs_purchases_goods.delete().where(
            docs_purchases_goods.c.docs_purchases_id == instance_id
        )
    )

    items_sum = 0.0
    for item in goods:
        item["docs_purchases_id"] = instance_id

        if item.get("price_type") is not None:
            if item["price_type"] not in price_types_cache:
                try:
                    await check_entity_exists(price_types, item["price_type"], user.id)
                    price_types_cache.add(item["price_type"])
                except HTTPException as e:
                    exceptions.append(str(item) + " " + e.detail)
                    continue
        if item.get("unit") is not None:
            if item["unit"] not in units_cache:
                try:
                    await check_unit_exists(item["unit"])
                    units_cache.add(item["unit"])
                except HTTPException as e:
                    exceptions.append(str(item) + " " + e.detail)
                    continue

        await database.execute(docs_purchases_goods.insert().values(item))
        items_sum += item["price"] * item["quantity"]

    return items_sum


@router.get("/docs_purchases/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)
    query = docs_purchases.select().where(
        docs_purchases.c.id == idx, docs_purchases.c.is_deleted.is_not(True)
    )
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail="Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)

    query = docs_purchases_goods.select().where(
        docs_purchases_goods.c.docs_purchases_id == idx
    )
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
    goods = []
    for good in goods_db:
        nomenclature_db = await database.fetch_one(
            nomenclature.select().where(nomenclature.c.id == good["nomenclature"])
        )
        unit_db = await database.fetch_one(
            units.select().where(units.c.id == good["unit"])
        )
        goods.append(
            {
                "price_type": good["price_type"],
                "price": good["price"],
                "quantity": good["quantity"],
                "unit": good["unit"],
                "nomenclature": good["nomenclature"],
                "unit_name": unit_db.name,
                "nomenclature_name": nomenclature_db.name,
            }
        )

    instance_db["goods"] = goods

    return instance_db


@router.get("/docs_purchases/", response_model=schemas.ViewResult)
async def get_list(token: str, limit: int = 100, offset: int = 0, tags: str = None):
    """Получение списка документов"""
    user = await get_user_by_token(token)
    filters_list = []
    if tags:
        filters_list.append(docs_warehouse.c.tags.ilike(f"%{tags}%"))

    query = (
        docs_purchases.select()
        .where(
            docs_purchases.c.is_deleted.is_not(True),
            docs_purchases.c.cashbox == user.cashbox_id,
        )
        .where(*filters_list)
        .order_by(desc(docs_purchases.c.id))
        .limit(limit)
        .offset(offset)
    )

    query_count = (
        select(func.count(docs_purchases.c.id))
        .where(
            docs_purchases.c.is_deleted.is_not(True),
            docs_purchases.c.cashbox == user.cashbox_id,
        )
        .where(*filters_list)
    )
    count = await database.fetch_one(query_count)

    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]
    return {"result": items_db, "count": count.count_1}


async def check_foreign_keys(instance_values, user, exceptions) -> bool:
    if instance_values.get("nomenclature") is not None:
        if instance_values["nomenclature"] not in nomenclature_cache:
            try:
                await check_entity_exists(
                    nomenclature, instance_values["nomenclature"], user.id
                )
                nomenclature_cache.add(instance_values["nomenclature"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("client") is not None:
        if instance_values["client"] not in contragents_cache:
            try:
                await check_contragent_exists(
                    instance_values["client"], user.cashbox_id
                )
                contragents_cache.add(instance_values["client"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contragent") is not None:
        if instance_values["contragent"] not in contragents_cache:
            try:
                await check_contragent_exists(
                    instance_values["contragent"], user.cashbox_id
                )
                contragents_cache.add(instance_values["contragent"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contract") is not None:
        if instance_values["contract"] not in contracts_cache:
            try:
                await check_entity_exists(
                    contracts, instance_values["contract"], user.id
                )
                contracts_cache.add(instance_values["contract"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

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

    if instance_values.get("purchased_by") is not None:
        if instance_values["purchased_by"] not in users_cache:
            query = users_cboxes_relation.select().where(
                users_cboxes_relation.c.id == instance_values["purchased_by"]
            )
            if not await database.fetch_one(query):
                exceptions.append(str(instance_values) + " Пользователь не существует!")
                return False
            users_cache.add(instance_values["purchased_by"])
    return True


@router.post("/docs_purchases/", response_model=schemas.ListView)
async def create(token: str, docs_purchases_data: schemas.CreateMass):
    """Создание документов"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    exceptions = []
    for instance_values in docs_purchases_data.dict()["__root__"]:
        instance_values["created_by"] = user.id
        instance_values["cashbox"] = user.cashbox_id
        instance_values["is_deleted"] = False
        if not await check_period_blocked(
            instance_values["organization"], instance_values.get("dated"), exceptions
        ):
            continue
        if not await check_foreign_keys(
            instance_values,
            user,
            exceptions,
        ):
            continue

        goods: list = instance_values.get("goods")
        try:
            del instance_values["goods"]
        except KeyError:
            pass
        goods = goods or []
        instance_id = None

        async with database.transaction():
            existing_id = await _find_existing_purchase_id(
                instance_values=instance_values,
                cashbox_id=user.cashbox_id,
            )

            if existing_id is not None:
                update_values = {
                    key: value
                    for key, value in instance_values.items()
                    if key not in {"created_by", "cashbox", "is_deleted"}
                }
                await database.execute(
                    docs_purchases.update()
                    .where(docs_purchases.c.id == existing_id)
                    .values(update_values)
                )
                instance_id = existing_id
            else:
                query = docs_purchases.insert().values(instance_values)
                instance_id = await database.execute(query)

            inserted_ids.add(instance_id)
            items_sum = await _replace_purchase_goods(
                instance_id=instance_id,
                goods=goods,
                user=user,
                exceptions=exceptions,
            )

            await database.execute(
                docs_purchases.update()
                .where(docs_purchases.c.id == instance_id)
                .values({"sum": items_sum})
            )

        # Синк incoming docs_warehouse выполняем ПОСЛЕ COMMIT внешней tx:
        # upsert_warehouse_doc сам открывает tx + advisory lock + делает
        # publish в RabbitMQ; держать всё это во внешней tx означает
        # idle-in-transaction на десятки секунд (см. DOCS_WAREHOUSE_TX_HANG.md).
        # Внимание: docs_purchases уже закоммичен к этому моменту. Если синк
        # упадёт — получим закоммиченный purchase без соответствующего
        # docs_warehouse. upsert_warehouse_doc идемпотентен, так что retry
        # клиентом доведёт состояние до консистентного; ошибку логируем,
        # чтобы не прятать её за 500.
        try:
            await _sync_purchase_incoming_doc(token, user.cashbox_id, instance_id)
        except Exception as e:
            print(
                f"[docs_purchases] failed to sync incoming docs_warehouse for purchase={instance_id}: {e}"
            )

        # --- RabbitMQ: авто-расход по закупке ---
        factory = ioc.get(IRabbitFactory)
        rabbitmq_messaging = await factory()

        await rabbitmq_messaging.publish(
            CreatePurchaseAutoExpenseMessage(
                message_id=uuid.uuid4(),
                token=token,
                cashbox_id=user.cashbox_id,
                purchase_id=instance_id,
            ),
            routing_key="purchase.auto_expense",
        )
        # --- end RabbitMQ ---
    renumber_query = text(
        """
        UPDATE docs_purchases dp
        SET number = sub.rn::text
        FROM (
            SELECT id, number, ROW_NUMBER() OVER (ORDER BY id) AS rn
            FROM docs_purchases
            WHERE cashbox = :cashbox_id AND is_deleted = false
        ) sub
        WHERE dp.id = sub.id AND sub.number IS NULL
    """
    ).bindparams(cashbox_id=user.cashbox_id)
    await database.execute(renumber_query)

    query = docs_purchases.select().where(docs_purchases.c.id.in_(inserted_ids))
    docs_purchases_db = await database.fetch_all(query)
    docs_purchases_db = [*map(datetime_to_timestamp, docs_purchases_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_purchases",
            "result": docs_purchases_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_purchases_db


@router.patch("/docs_purchases/", response_model=schemas.ListView)
async def update(token: str, docs_purchases_data: schemas.EditMass):
    """Редактирование документов"""
    user = await get_user_by_token(token)

    updated_ids = set()
    exceptions = []
    for instance_values in docs_purchases_data.dict(exclude_unset=True)["__root__"]:
        if not await check_period_blocked(
            instance_values["organization"], instance_values.get("dated"), exceptions
        ):
            continue
        if not await check_foreign_keys(instance_values, user, exceptions):
            continue

        goods: list = instance_values.get("goods")
        try:
            del instance_values["goods"]
        except KeyError:
            pass

        query = (
            docs_purchases.update()
            .where(docs_purchases.c.id == instance_values["id"])
            .values(instance_values)
        )
        await database.execute(query)

        instance_id = instance_values["id"]
        updated_ids.add(instance_id)

        if goods is not None:
            query = docs_purchases_goods.delete().where(
                docs_purchases_goods.c.docs_purchases_id == instance_id
            )
            await database.execute(query)

            items_sum = 0
            for item in goods:
                item["docs_purchases_id"] = instance_id

                if item.get("price_type") is not None:
                    if item["price_type"] not in price_types_cache:
                        try:
                            await check_entity_exists(
                                price_types, item["price_type"], user.id
                            )
                            price_types_cache.add(item["price_type"])
                        except HTTPException as e:
                            exceptions.append(str(item) + " " + e.detail)
                            continue

                if item.get("unit") is not None:
                    if item["unit"] not in units_cache:
                        try:
                            await check_unit_exists(item["unit"])
                            units_cache.add(item["unit"])
                        except HTTPException as e:
                            exceptions.append(str(item) + " " + e.detail)
                            continue

                query = docs_purchases_goods.insert().values(item)
                await database.execute(query)

                items_sum += item["price"] * item["quantity"]

            query = (
                docs_purchases.update()
                .where(docs_purchases.c.id == instance_id)
                .values({"sum": items_sum})
            )
            await database.execute(query)

        try:
            await _sync_purchase_incoming_doc(token, user.cashbox_id, instance_id)
        except Exception as e:
            print(
                f"[docs_purchases] failed to sync incoming docs_warehouse for purchase={instance_id}: {e}"
            )

    query = docs_purchases.select().where(docs_purchases.c.id.in_(updated_ids))
    docs_purchases_db = await database.fetch_all(query)
    docs_purchases_db = [*map(datetime_to_timestamp, docs_purchases_db)]

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "docs_purchases",
            "result": docs_purchases_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_purchases_db


@router.delete("/docs_purchases/", response_model=schemas.ListView)
async def delete(token: str, ids: list[int]):
    """Удаление документов"""
    user = await get_user_by_token(token)

    query = docs_purchases.select().where(
        docs_purchases.c.id.in_(ids),
        docs_purchases.c.is_deleted.is_not(True),
        docs_purchases.c.cashbox == user.cashbox_id,
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_purchases.update()
            .where(
                docs_purchases.c.id.in_(ids),
                docs_purchases.c.is_deleted.is_not(True),
                docs_purchases.c.cashbox == user.cashbox_id,
            )
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_purchases",
                "result": items_db,
            },
        )

    return items_db
