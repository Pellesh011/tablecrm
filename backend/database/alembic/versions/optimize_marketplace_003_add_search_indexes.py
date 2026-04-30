"""Add search indexes for marketplace filters.

Revision ID: optimize_marketplace_003
Revises: merge_videos_mp_orders
Create Date: 2026-03-16
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "optimize_marketplace_003"
down_revision = "merge_videos_mp_orders"
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
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = set(inspector.get_table_names())

    # For ILIKE with leading wildcards.
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    if "nomenclature" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_nomenclature_name_trgm",
            """
            CREATE INDEX CONCURRENTLY idx_nomenclature_name_trgm
            ON nomenclature USING gin (lower(name) gin_trgm_ops)
            """,
        )
    if "manufacturers" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_manufacturers_name_trgm",
            """
            CREATE INDEX CONCURRENTLY idx_manufacturers_name_trgm
            ON manufacturers USING gin (lower(name) gin_trgm_ops)
            """,
        )
    if "cashboxes" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_cashboxes_name_trgm",
            """
            CREATE INDEX CONCURRENTLY idx_cashboxes_name_trgm
            ON cashboxes USING gin (lower(name) gin_trgm_ops)
            """,
        )
    if "users" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_users_phone_trgm",
            """
            CREATE INDEX CONCURRENTLY idx_users_phone_trgm
            ON users USING gin (lower(phone_number) gin_trgm_ops)
            WHERE phone_number IS NOT NULL
            """,
        )
    if "prices" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_prices_address_trgm",
            """
            CREATE INDEX CONCURRENTLY idx_prices_address_trgm
            ON prices USING gin (lower(address) gin_trgm_ops)
            WHERE address IS NOT NULL AND address <> ''
            """,
        )
    if "warehouses" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_warehouses_address_trgm",
            """
            CREATE INDEX CONCURRENTLY idx_warehouses_address_trgm
            ON warehouses USING gin (lower(address) gin_trgm_ops)
            WHERE address IS NOT NULL AND address <> ''
            """,
        )
    if "nomenclature_attributes_value" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_nomenclature_attr_value_trgm",
            """
            CREATE INDEX CONCURRENTLY idx_nomenclature_attr_value_trgm
            ON nomenclature_attributes_value USING gin (lower(value) gin_trgm_ops)
            """,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_nomenclature_name_trgm")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_manufacturers_name_trgm")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cashboxes_name_trgm")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_users_phone_trgm")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_prices_address_trgm")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_warehouses_address_trgm")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_nomenclature_attr_value_trgm")
