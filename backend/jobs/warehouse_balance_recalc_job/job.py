"""
Consumer для пересчёта warehouse_balances_latest.
Вызывается при каждом изменении документа, затрагивающего остатки.
"""

import logging
from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from database.db import (
    OperationType,
    database,
    warehouse_balances_latest,
    warehouse_register_movement,
)
from messages.warehouse import WarehouseBalanceRecalcEvent
from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)


async def recalc_balance(event: WarehouseBalanceRecalcEvent) -> None:
    signed_amount = case(
        (
            warehouse_register_movement.c.type_amount == OperationType.plus,
            warehouse_register_movement.c.amount,
        ),
        else_=-warehouse_register_movement.c.amount,
    )

    q = select(
        func.coalesce(func.sum(signed_amount), 0).label("current_amount"),
        func.coalesce(
            func.sum(
                case(
                    (
                        warehouse_register_movement.c.type_amount == OperationType.plus,
                        warehouse_register_movement.c.amount,
                    ),
                    else_=0,
                )
            ),
            0,
        ).label("incoming_amount"),
        func.coalesce(
            func.sum(
                case(
                    (
                        warehouse_register_movement.c.type_amount
                        == OperationType.minus,
                        warehouse_register_movement.c.amount,
                    ),
                    else_=0,
                )
            ),
            0,
        ).label("outgoing_amount"),
    ).where(
        warehouse_register_movement.c.organization_id == event.organization_id,
        warehouse_register_movement.c.warehouse_id == event.warehouse_id,
        warehouse_register_movement.c.nomenclature_id == event.nomenclature_id,
    )

    row = await database.fetch_one(q)
    if row is None:
        current_amount = 0
        incoming_amount = 0
        outgoing_amount = 0
    else:
        current_amount = row["current_amount"]
        incoming_amount = row["incoming_amount"]
        outgoing_amount = row["outgoing_amount"]

    stmt = (
        pg_insert(warehouse_balances_latest)
        .values(
            organization_id=event.organization_id,
            warehouse_id=event.warehouse_id,
            nomenclature_id=event.nomenclature_id,
            cashbox_id=event.cashbox_id,
            current_amount=current_amount,
            incoming_amount=incoming_amount,
            outgoing_amount=outgoing_amount,
        )
        .on_conflict_do_update(
            index_elements=[
                warehouse_balances_latest.c.organization_id,
                warehouse_balances_latest.c.warehouse_id,
                warehouse_balances_latest.c.nomenclature_id,
            ],
            set_=dict(
                cashbox_id=event.cashbox_id,
                current_amount=current_amount,
                incoming_amount=incoming_amount,
                outgoing_amount=outgoing_amount,
                updated_at=func.now(),
            ),
        )
    )
    await database.execute(stmt)

    logger.info(
        "warehouse_balances_latest updated: org=%s wh=%s nom=%s current=%.4f",
        event.organization_id,
        event.warehouse_id,
        event.nomenclature_id,
        float(current_amount),
    )


class WarehouseBalanceRecalcHandler(IEventHandler[WarehouseBalanceRecalcEvent]):
    async def __call__(
        self,
        event: Mapping[str, Any],
        message: Optional[IncomingMessage] = None,
    ):
        await recalc_balance(WarehouseBalanceRecalcEvent(**event))
