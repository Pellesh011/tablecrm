"""
Периодическая сверка warehouse_balances_latest с aggregate из register.
Запускается раз в 10 минут. Исправляет расхождения, возникшие из-за
потери RabbitMQ-событий или любых edge-case'ов.
"""

import logging

from database.db import database

logger = logging.getLogger(__name__)

RECONCILE_SQL = """
INSERT INTO warehouse_balances_latest
    (
        organization_id,
        warehouse_id,
        nomenclature_id,
        cashbox_id,
        current_amount,
        incoming_amount,
        outgoing_amount,
        updated_at
    )
SELECT
    r.organization_id,
    r.warehouse_id,
    r.nomenclature_id,
    (
        SELECT r2.cashbox_id
        FROM warehouse_register_movement r2
        WHERE r2.organization_id = r.organization_id
          AND r2.warehouse_id = r.warehouse_id
          AND r2.nomenclature_id = r.nomenclature_id
        ORDER BY r2.id DESC
        LIMIT 1
    ) AS cashbox_id,
    COALESCE(SUM(CASE WHEN r.type_amount = 'plus' THEN r.amount ELSE -r.amount END), 0),
    COALESCE(SUM(CASE WHEN r.type_amount = 'plus' THEN r.amount ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN r.type_amount = 'minus' THEN r.amount ELSE 0 END), 0),
    now()
FROM warehouse_register_movement r
WHERE r.organization_id IS NOT NULL
  AND r.warehouse_id IS NOT NULL
  AND r.nomenclature_id IS NOT NULL
GROUP BY r.organization_id, r.warehouse_id, r.nomenclature_id
ON CONFLICT (organization_id, warehouse_id, nomenclature_id)
DO UPDATE SET
    current_amount = EXCLUDED.current_amount,
    incoming_amount = EXCLUDED.incoming_amount,
    outgoing_amount = EXCLUDED.outgoing_amount,
    cashbox_id = EXCLUDED.cashbox_id,
    updated_at = now()
WHERE
    ABS(warehouse_balances_latest.current_amount - EXCLUDED.current_amount) > 0.001
    OR ABS(warehouse_balances_latest.incoming_amount - EXCLUDED.incoming_amount) > 0.001
    OR ABS(warehouse_balances_latest.outgoing_amount - EXCLUDED.outgoing_amount) > 0.001
"""


async def reconcile_warehouse_balances() -> None:
    await database.execute(RECONCILE_SQL)
    logger.info("warehouse_balances reconcile completed")
