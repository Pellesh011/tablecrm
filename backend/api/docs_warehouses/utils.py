from typing import Optional

from api.docs_warehouses.func_warehouse import (
    acquire_docs_warehouse_lock,
    call_type_movement,
    set_data_doc_warehouse,
    update_docs_warehouse,
    update_goods_warehouse,
)
from database.db import (
    OperationType,
    database,
    docs_warehouse,
    docs_warehouse_goods,
)
from functions.helpers import datetime_to_timestamp
from functions.warehouse_events import publish_balance_recalc_batch
from sqlalchemy import and_, desc, select
from ws_manager import manager


async def _find_existing_warehouse_doc_id(doc: dict, cashbox_id: int) -> Optional[int]:
    operation = doc.get("operation")
    if not operation:
        return None

    purchase_id = doc.get("docs_purchases")
    sales_id = doc.get("docs_sales_id")

    filters = [
        docs_warehouse.c.cashbox == cashbox_id,
        docs_warehouse.c.is_deleted.is_not(True),
        docs_warehouse.c.operation == operation,
    ]

    if purchase_id is not None:
        filters.append(docs_warehouse.c.docs_purchases == int(purchase_id))
    elif sales_id is not None:
        filters.append(docs_warehouse.c.docs_sales_id == int(sales_id))
    else:
        number = doc.get("number")
        if not number:
            return None

        filters.append(docs_warehouse.c.number == str(number))

    row = await database.fetch_one(
        select(docs_warehouse.c.id)
        .where(and_(*filters))
        .order_by(desc(docs_warehouse.c.id))
    )
    return int(row["id"]) if row else None


async def upsert_warehouse_doc(token: str, doc: dict, cashbox_id: int) -> int:
    if doc is None:
        raise TypeError("upsert_warehouse_doc: doc must not be None")

    if not isinstance(doc, dict):
        raise TypeError(f"upsert_warehouse_doc: expected dict, got {type(doc)}")

    doc = dict(doc)
    response = None

    pending_events: list = []
    existing_id = await _find_existing_warehouse_doc_id(doc, cashbox_id)
    if existing_id:
        if doc.get("number") is None:
            doc.pop("number", None)
        doc["id"] = existing_id

        # Вся update-ветка (stored read, update header, rewrite goods+movements)
        # должна идти под одной транзакцией, чтобы pg_advisory_xact_lock держался
        # до COMMIT и сериализовал параллельные upsert'ы по одному doc_id.
        # publish в RabbitMQ делаем ПОСЛЕ COMMIT — см. DOCS_WAREHOUSE_TX_HANG.md.
        async with database.transaction():
            await acquire_docs_warehouse_lock(existing_id)

            goods = doc.get("goods")
            if goods is None:
                goods = await database.fetch_all(
                    docs_warehouse_goods.select().where(
                        docs_warehouse_goods.c.docs_warehouse_id == existing_id
                    )
                )

            stored_item_data = await database.fetch_one(
                docs_warehouse.select().where(docs_warehouse.c.id == existing_id)
            )
            if stored_item_data:
                merged_doc = dict(stored_item_data)
                merged_doc.update(doc)
                merged_doc.pop("goods", None)

                entity = await set_data_doc_warehouse(
                    entity_values=merged_doc, token=token
                )
                doc_id = await update_docs_warehouse(entity=entity)
                entity.update({"goods": goods})

                if entity["operation"] == "incoming":
                    events = await update_goods_warehouse(
                        entity=entity, doc_id=doc_id, type_operation=OperationType.plus
                    )
                    if events:
                        pending_events.extend(events)
                elif entity["operation"] == "outgoing":
                    events = await update_goods_warehouse(
                        entity=entity, doc_id=doc_id, type_operation=OperationType.minus
                    )
                    if events:
                        pending_events.extend(events)
                elif entity["operation"] == "transfer":
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
                elif entity["operation"] == "write_off":
                    events = await update_goods_warehouse(
                        entity=entity, doc_id=doc_id, type_operation=OperationType.minus
                    )
                    if events:
                        pending_events.extend(events)

                response = doc_id

    if response is None:
        # call_type_movement сам опубликует события после своей внутренней tx.
        response = await call_type_movement(
            doc["operation"], entity_values=doc, token=token
        )
    elif pending_events:
        await publish_balance_recalc_batch(pending_events)

    return response


async def create_warehouse_docs(token: str, doc, cashbox_id: int):
    """
    Идемпотентное создание/обновление складского документа.
    Сначала ищем документ по docs_purchases/docs_sales_id, а если их нет —
    по стабильной сигнатуре number + operation + cashbox.
    Это предотвращает дубли при ретраях и позволяет безопасно повторять create.
    """
    if doc is None:
        return []

    response = await upsert_warehouse_doc(token=token, doc=doc, cashbox_id=cashbox_id)

    query = docs_warehouse.select().where(docs_warehouse.c.id == response)
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
