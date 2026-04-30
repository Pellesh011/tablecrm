"""add warehouse register indexes for balance recalculation

Revision ID: d4e5f6a7b8c9
Revises: b3c4d5e6f7a8
Create Date: 2026-04-04 14:30:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e5f6a7b8c9"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def _create_index_concurrently_if_not_exists(index_name: str, ddl: str) -> None:
    conn = op.get_bind()
    exists = conn.scalar(
        sa.text("SELECT to_regclass(:n)"), {"n": f"public.{index_name}"}
    )
    if exists is None:
        with op.get_context().autocommit_block():
            op.execute(ddl)


def upgrade() -> None:
    # Основной индекс под worker/reconcile:
    # фильтр по organization_id + warehouse_id + nomenclature_id
    # и быстрый поиск последней записи по id DESC.
    _create_index_concurrently_if_not_exists(
        "idx_wrm_org_wh_nom_id_desc",
        """
        CREATE INDEX CONCURRENTLY idx_wrm_org_wh_nom_id_desc
        ON warehouse_register_movement (organization_id, warehouse_id, nomenclature_id, id DESC)
        """,
    )

    # Индекс под выборки склада с датами и товаром:
    # alt_warehouse_balances и clearQuantity.
    _create_index_concurrently_if_not_exists(
        "idx_wrm_wh_nom_created_at",
        """
        CREATE INDEX CONCURRENTLY idx_wrm_wh_nom_created_at
        ON warehouse_register_movement (warehouse_id, nomenclature_id, created_at)
        """,
    )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_wrm_org_wh_nom_id_desc")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_wrm_wh_nom_created_at")
