"""add docs_warehouse hotfix indexes concurrently

Revision ID: c1f9a7e24d10
Revises: merge_heads_003
Create Date: 2026-02-27 19:20:00.000000
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c1f9a7e24d10"
down_revision = "merge_heads_003"
branch_labels = None
depends_on = None


def _create_index_concurrently_if_not_exists(index_name: str, ddl: str) -> None:
    # В проде миграции могут запускаться повторно: проверяем индекс перед созданием.
    conn = op.get_bind()
    exists = conn.scalar(
        sa.text("SELECT to_regclass(:n)"), {"n": f"public.{index_name}"}
    )
    if exists is None:
        # CREATE INDEX CONCURRENTLY нельзя выполнять внутри транзакции.
        with op.get_context().autocommit_block():
            op.execute(ddl)


def upgrade() -> None:
    # Ускоряет idempotency-поиск docs_warehouse по закупкам.
    _create_index_concurrently_if_not_exists(
        "idx_docs_warehouse_cashbox_deleted_operation_purchase",
        """
        CREATE INDEX CONCURRENTLY idx_docs_warehouse_cashbox_deleted_operation_purchase
        ON docs_warehouse (cashbox, is_deleted, operation, docs_purchases)
        """,
    )

    # Ускоряет idempotency-поиск docs_warehouse по продажам.
    _create_index_concurrently_if_not_exists(
        "idx_docs_warehouse_cashbox_deleted_operation_sales",
        """
        CREATE INDEX CONCURRENTLY idx_docs_warehouse_cashbox_deleted_operation_sales
        ON docs_warehouse (cashbox, is_deleted, operation, docs_sales_id)
        """,
    )

    # Ускоряет чтение/удаление товаров по документу склада.
    _create_index_concurrently_if_not_exists(
        "idx_docs_warehouse_goods_doc_id",
        """
        CREATE INDEX CONCURRENTLY idx_docs_warehouse_goods_doc_id
        ON docs_warehouse_goods (docs_warehouse_id)
        """,
    )

    # Ускоряет очистку и выборку движений по документу и типу операции.
    _create_index_concurrently_if_not_exists(
        "idx_warehouse_register_movement_doc_type",
        """
        CREATE INDEX CONCURRENTLY idx_warehouse_register_movement_doc_type
        ON warehouse_register_movement (document_warehouse_id, type_amount)
        """,
    )

    # Ускоряет выбор последних остатков по связке склад/номенклатура/организация.
    _create_index_concurrently_if_not_exists(
        "idx_warehouse_balances_wh_nom_org_created_desc",
        """
        CREATE INDEX CONCURRENTLY idx_warehouse_balances_wh_nom_org_created_desc
        ON warehouse_balances (warehouse_id, nomenclature_id, organization_id, created_at DESC)
        """,
    )


def downgrade() -> None:
    # DROP INDEX CONCURRENTLY также выполняем вне транзакции.
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_warehouse_cashbox_deleted_operation_purchase"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_warehouse_cashbox_deleted_operation_sales"
        )
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_docs_warehouse_goods_doc_id")
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_warehouse_register_movement_doc_type"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_warehouse_balances_wh_nom_org_created_desc"
        )
