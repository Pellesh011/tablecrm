from typing import Any, Dict

from database.db import users_cboxes_relation
from sqlalchemy.orm import Session

from .models import TechOperationDB


async def _tech_operations_cashbox_scope_query(db: Session, cashbox_id: int):
    """
    Метод позволяет получить операции тех карт только для определенной кассы
    """
    return (
        db.query(TechOperationDB)
        .join(
            users_cboxes_relation,
            TechOperationDB.user_id == users_cboxes_relation.c.id,
        )
        .filter(
            TechOperationDB.status != "deleted",
            users_cboxes_relation.c.cashbox_id == cashbox_id,
        )
    )


def _serialize_tech_operation(op) -> Dict[str, Any]:
    """Корректная сериализация TechOperationDB → dict для Pydantic"""
    return {
        "id": op.id,
        "tech_card_id": op.tech_card_id,
        "output_quantity": op.output_quantity,
        "from_warehouse_id": op.from_warehouse_id,
        "to_warehouse_id": op.to_warehouse_id,
        "nomenclature_id": op.nomenclature_id,
        "user_id": op.user_id,
        "cashbox_id": op.cashbox_id,
        "status": op.status,
        "production_doc_id": op.production_doc_id,
        "consumption_doc_id": op.consumption_doc_id,
        "docs_sales_id": op.docs_sales_id,
        "created_at": op.created_at,
        "updated_at": op.updated_at,
        "production_order_id": op.production_order_id,
        "consumption_order_id": op.consumption_order_id,
        "sale_write_off_doc_id": op.sale_write_off_doc_id,
        "component_quantities": [
            {
                "id": comp.id,
                "operation_id": comp.operation_id,
                "nomeclature_id": comp.nomeclature_id,
                "name": comp.name,
                "quantity": comp.quantity,
                "gross_weight": comp.gross_weight,
                "net_weight": comp.net_weight,
            }
            for comp in (op.components or [])
        ],
        "payment_ids": [p.payment_id for p in (op.payments or [])],
    }
