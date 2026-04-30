"""Add payments updated_at sync index.

Revision ID: 8f3b1e9c4d2a
Revises: e6f7a8b9c0d1
Create Date: 2026-04-05
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8f3b1e9c4d2a"
down_revision = "e6f7a8b9c0d1"
branch_labels = None
depends_on = None


def _create_index_concurrently_if_not_exists(index_name: str, ddl: str) -> None:
    conn = op.get_bind()
    exists = conn.scalar(
        sa.text("SELECT to_regclass(:name)"),
        {"name": f"public.{index_name}"},
    )
    if exists is None:
        with op.get_context().autocommit_block():
            op.execute(ddl)


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if "payments" not in set(inspector.get_table_names()):
        return

    _create_index_concurrently_if_not_exists(
        "idx_payments_cashbox_updated_id_sync",
        """
        CREATE INDEX CONCURRENTLY idx_payments_cashbox_updated_id_sync
        ON payments (cashbox, updated_at, id)
        WHERE is_deleted = false AND parent_id IS NULL
        """,
    )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_payments_cashbox_updated_id_sync"
        )
