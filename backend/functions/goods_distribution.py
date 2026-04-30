import json
from datetime import datetime, timedelta

from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc
from const import SaleOperations, WarehouseOperations
from database.db import (
    database,
    distribution_docs,
    distribution_docs_operations,
    docs_purchases,
    docs_purchases_goods,
    docs_sales,
    docs_sales_goods,
    docs_warehouse,
    docs_warehouse_goods,
    fifo_settings,
    organizations,
)


async def distribute(
    organization_id: int,
    period_start: int = 0,
    period_end: int = None,
    mode: str = "closing",
):
    if await is_organization_in_progress(organization_id):
        return
    query = organizations.select().where(organizations.c.id == organization_id)
    organization = await database.fetch_one(query)
    if not organization:
        raise Exception(f"No such organization: {organization_id}")

    await set_organization_in_progress(organization_id)
    try:
        query = docs_sales.select().where(
            docs_sales.c.organization == organization_id,
            docs_sales.c.operation != SaleOperations.order,
            docs_sales.c.dated > period_start,
            docs_sales.c.dated < period_end if period_end else True,
        )
        sale_docs = await database.fetch_all(query)

        query = docs_purchases.select().where(
            docs_purchases.c.organization == organization_id,
            docs_purchases.c.dated > period_start,
            docs_purchases.c.dated < period_end if period_end else True,
        )
        purchase_docs = await database.fetch_all(query)

        query = docs_warehouse.select().where(
            docs_warehouse.c.organization == organization_id,
            docs_warehouse.c.dated > period_start,
            docs_warehouse.c.dated < period_end if period_end else True,
            docs_warehouse.c.operation.in_(
                (
                    WarehouseOperations.movement_in,
                    WarehouseOperations.surplus_posting,
                )
            ),
        )
        warehouse_docs_added = await database.fetch_all(query)

        query = docs_warehouse.select().where(
            docs_warehouse.c.organization == organization_id,
            docs_warehouse.c.dated > period_start,
            docs_warehouse.c.dated < period_end if period_end else True,
            docs_warehouse.c.operation.in_(
                (
                    WarehouseOperations.movement_out,
                    WarehouseOperations.internal_consumption,
                    WarehouseOperations.write_off,
                )
            ),
        )
        warehouse_docs_left = await database.fetch_all(query)

        sale_docs_ids = [doc.id for doc in sale_docs]
        purchase_docs_ids = [doc.id for doc in purchase_docs]
        warehouse_docs_added_ids = [doc.id for doc in warehouse_docs_added]
        warehouse_docs_left_ids = [doc.id for doc in warehouse_docs_left]

        query = docs_sales_goods.select().where(
            docs_sales_goods.c.docs_sales_id.in_(sale_docs_ids)
        )
        sold_items = await database.fetch_all(query)

        query = docs_purchases_goods.select().where(
            docs_purchases_goods.c.docs_purchases_id.in_(purchase_docs_ids)
        )
        purchased_items = await database.fetch_all(query)

        query = docs_warehouse_goods.select().where(
            docs_warehouse_goods.c.docs_warehouse_id.in_(warehouse_docs_left_ids),
        )
        warehouse_left_items = await database.fetch_all(query)
        query = docs_warehouse_goods.select().where(
            docs_warehouse_goods.c.docs_warehouse_id.in_(warehouse_docs_added_ids),
        )
        warehouse_added_items = await database.fetch_all(query)

        distribution_doc = {
            "organization": organization_id,
            "period_start": period_start,
            "period_end": period_end or datetime.utcnow().timestamp(),
        }
        query = distribution_docs.insert().values(distribution_doc)
        distribution_doc_id = await database.execute(query)

        table: list[dict] = []
        for item in purchased_items + warehouse_added_items:
            result = {
                "distribution_fifo": distribution_doc_id,
                "nomenclature": item.nomenclature,
                "dated": item.created_at.timestamp(),
                "start_amount": item.quantity,
                "start_price": item.price,
                "outgoing_amount": 0,
                "outgoing_price": 0,
                "end_amount": item.quantity,
                "end_price": item.price,
            }
            if hasattr(item, "docs_purchases_id"):
                result["document_purchase"] = item.docs_purchases_id
            elif hasattr(item, "docs_warehouse_id"):
                result["document_warehouse"] = item.docs_warehouse_id
            else:
                raise Exception("No docs_purchases_id or docs_warehouse_id attribute")
            table.append(result)
        table.sort(key=lambda x: x["dated"])

        added_items = {}
        for item in table:
            if added_items.get(item["nomenclature"]):
                added_items[item["nomenclature"]].append(item)
            else:
                added_items[item["nomenclature"]] = [item]

        for item in sold_items + warehouse_left_items:
            nomenclature_items = added_items.get(item.nomenclature)
            if not nomenclature_items:
                continue
            item_amount = item.quantity
            for record in nomenclature_items:
                if record["end_amount"] == 0:
                    continue
                elif item_amount > record["end_amount"]:
                    difference_amount = item_amount - record["end_amount"]
                    part_price = item.price / item.quantity * difference_amount
                    item_amount -= difference_amount
                    record["end_amount"] = 0
                    record["end_price"] -= part_price
                    record["outgoing_amount"] += difference_amount
                    record["outgoing_price"] += part_price
                else:
                    part_price = item.price / item.quantity * item_amount
                    record["end_amount"] -= item_amount
                    record["end_price"] -= part_price
                    record["outgoing_amount"] += item_amount
                    record["outgoing_price"] += part_price
                    break
            result = {
                "distribution_fifo": distribution_doc_id,
                "nomenclature": item.nomenclature,
                "dated": item.created_at.timestamp(),
                "start_amount": item.quantity,
                "start_price": item.price,
                "end_amount": item.quantity,
                "end_price": item.price,
            }
            if hasattr(item, "docs_sales_id"):
                result["document_sale"] = item.docs_sales_id
            elif hasattr(item, "docs_warehouse_id"):
                result["document_warehouse"] = item.docs_warehouse_id
            else:
                raise Exception("No docs_sales_id or docs_warehouse_id attribute")
            table.append(result)

        query = distribution_docs_operations.insert()
        await database.execute_many(query, table)

        if mode == "preview":
            if distribution_doc_id is None:
                return
            query = (
                fifo_settings.update()
                .where(fifo_settings.c.organization_id == organization_id)
                .values({"temporary_closed_date": period_end})
            )
            await database.execute(query)
            query = distribution_docs.delete().where(
                distribution_docs.c.is_preview == True,
                distribution_docs.c.organization == organization_id,
                distribution_docs.c.id != distribution_doc_id,
            )
            await database.execute(query)
            query = (
                distribution_docs.update()
                .where(distribution_docs.c.id == distribution_doc_id)
                .values({"is_preview": True})
            )
            await database.execute(query)
        elif mode == "closing":
            query = (
                fifo_settings.update()
                .where(fifo_settings.c.organization_id == organization_id)
                .values(
                    {
                        "fully_closed_date": period_end,
                        "blocked_date": period_end,
                    }
                )
            )
            await database.execute(query)

    finally:
        await set_organization_in_progress(organization_id, False)


async def process_distribution():
    query = fifo_settings.select()
    settings = await database.fetch_all(query)
    for setting in settings:
        first_day = datetime(datetime.utcnow().year, datetime.utcnow().month, 1)
        if setting.month_closing_delay_days:
            first_day += timedelta(days=setting.month_closing_delay_days)
        if setting.fully_closed_date < first_day.timestamp():
            first_day_timestamp = int(first_day.timestamp())
            await produce_distribution(
                organization_id=setting.organization_id,
                period_start=setting.fully_closed_date,
                period_end=int(first_day_timestamp),
            )
            continue

        previous_close_time = (
            int(datetime.utcnow().timestamp()) - setting.preview_close_period_seconds
        )
        if (
            not setting.temporary_closed_date
            or setting.temporary_closed_date <= previous_close_time
        ):
            now_timestamp = int(datetime.utcnow().timestamp())
            await produce_distribution(
                organization_id=setting.organization_id,
                period_start=setting.fully_closed_date,
                period_end=now_timestamp,
            )


async def is_organization_in_progress(org_id: int):
    query = fifo_settings.select().where(
        fifo_settings.c.organization_id == org_id,
        fifo_settings.c.in_progress == True,
    )
    if await database.fetch_one(query):
        return True
    else:
        return False


async def set_organization_in_progress(org_id: int, status: bool = True):
    query = (
        fifo_settings.update()
        .where(fifo_settings.c.organization_id == org_id)
        .values({"in_progress": status})
    )
    await database.execute(query)


async def produce_distribution(**body) -> None:
    factory = ioc.get(IRabbitFactory)
    messaging = await factory()
    body["func_name"] = distribute.__name__
    await messaging.publish_to_queue("report_queue", json.dumps(body).encode())
