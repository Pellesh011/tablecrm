import logging
from datetime import datetime

from apps.cdek.models import cdek_order_status_history, cdek_orders
from apps.cdek.utils import (
    get_cdek_credentials,
    get_or_create_cdek_integration,
    integration_info,
)
from database.db import database, users_cboxes_relation
from sqlalchemy import and_, select
from ws_manager import manager

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"DELIVERED", "NOT_DELIVERED", "RETURNED", "CANCELLED"}


async def sync_cdek_order_status(order_uuid: str, client, cashbox_id: int):
    """Синхронизирует статус одного заказа. Возвращает True если статус изменился."""
    try:
        result = await client.get_order_by_uuid(order_uuid)
    except Exception as e:
        logger.warning(f"Failed to fetch CDEK order {order_uuid}: {e}")
        return False

    entity = result.get("entity", {})
    if not entity:
        return False

    statuses = entity.get("statuses", [])
    if not statuses:
        return False

    latest = statuses[-1]
    new_status = latest.get("code")
    status_name = latest.get("name")
    status_date = latest.get("date_time")
    city = latest.get("city")

    current = await database.fetch_one(
        cdek_orders.select().where(cdek_orders.c.order_uuid == order_uuid)
    )
    if not current:
        return False

    if current["status"] == new_status:
        return False

    await database.execute(
        cdek_orders.update()
        .where(cdek_orders.c.order_uuid == order_uuid)
        .values(
            status=new_status,
            status_date=status_date,
            updated_at=datetime.utcnow(),
        )
    )

    existing_history = await database.fetch_one(
        select(cdek_order_status_history).where(
            and_(
                cdek_order_status_history.c.order_uuid == order_uuid,
                cdek_order_status_history.c.status_code == new_status,
            )
        )
    )
    if not existing_history:
        await database.execute(
            cdek_order_status_history.insert().values(
                order_uuid=order_uuid,
                status_code=new_status,
                status_name=status_name,
                date_time=status_date or datetime.utcnow(),
                city=city,
            )
        )

    users = await database.fetch_all(
        users_cboxes_relation.select().where(
            users_cboxes_relation.c.cashbox_id == cashbox_id
        )
    )
    ws_payload = {
        "target": "cdek_status",
        "action": "update",
        "order_uuid": order_uuid,
        "status": new_status,
        "status_name": status_name,
        "doc_sales_id": current["doc_sales_id"],
        "city": city,
        "date_time": str(status_date) if status_date else None,
    }
    for user in users:
        try:
            await manager.send_message(user["token"], ws_payload)
        except Exception:
            pass

    logger.info(f"Order {order_uuid}: {current['status']} → {new_status}")
    return True


async def sync_all_active_orders():
    """
    Крон-задача: синхронизирует статусы всех незавершённых заказов.
    Запускать каждые 15-30 минут.
    """
    active_orders = await database.fetch_all(
        cdek_orders.select().where(cdek_orders.c.status.notin_(TERMINAL_STATUSES))
    )

    if not active_orders:
        return

    logger.info(f"Syncing {len(active_orders)} active CDEK orders...")

    by_cashbox: dict[int, list] = {}
    for order in active_orders:
        by_cashbox.setdefault(order["cashbox_id"], []).append(order)

    integration_id = await get_or_create_cdek_integration()
    updated_count = 0

    for cashbox_id, orders in by_cashbox.items():
        integ = await integration_info(cashbox_id, integration_id)
        if not integ:
            continue

        creds = await get_cdek_credentials(integ["id"])
        if not creds:
            continue

        from apps.cdek.client import CdekClient

        client = CdekClient(creds["account"], creds["secure_password"])

        for order in orders:
            changed = await sync_cdek_order_status(
                order["order_uuid"], client, cashbox_id
            )
            if changed:
                updated_count += 1

    logger.info(f"CDEK sync complete: {updated_count}/{len(active_orders)} updated")
