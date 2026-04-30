"""Add core marketplace indexes.

Revision ID: optimize_marketplace_004
Revises: optimize_marketplace_003
Create Date: 2026-03-16
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "optimize_marketplace_004"
down_revision = "optimize_marketplace_003"
branch_labels = None
depends_on = None


def _create_index_concurrently_if_not_exists(index_name: str, ddl: str) -> None:
    conn = op.get_bind()
    exists = conn.scalar(
        sa.text("SELECT to_regclass(:n)"), {"n": f"public.{index_name}"}
    )
    if exists is None:
        # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
        with op.get_context().autocommit_block():
            op.execute(ddl)


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = set(inspector.get_table_names())

    if "docs_sales_goods" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_docs_sales_goods_nomenclature",
            """
            CREATE INDEX CONCURRENTLY idx_docs_sales_goods_nomenclature
            ON docs_sales_goods (nomenclature)
            """,
        )
    if "warehouse_balances" in tables:
        # Пропускаем, если эквивалентный индекс уже существует под другим именем.
        existing_equivalent = conn.scalar(
            sa.text("SELECT to_regclass(:n)"),
            {"n": "public.idx_warehouse_balances_org_wh_nom_id_desc"},
        )
        if existing_equivalent is None:
            _create_index_concurrently_if_not_exists(
                "idx_balances_latest",
                """
                CREATE INDEX CONCURRENTLY idx_balances_latest
                ON warehouse_balances (organization_id, warehouse_id, nomenclature_id, id DESC)
                """,
            )
    if "pictures" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_pictures_nomenclature",
            """
            CREATE INDEX CONCURRENTLY idx_pictures_nomenclature
            ON pictures (entity_id)
            WHERE entity = 'nomenclature' AND is_deleted IS NOT TRUE
            """,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_goods_nomenclature"
        )
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_balances_latest")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_pictures_nomenclature")
